# coding=utf-8
"""Phase 1 happy-path test: integrate a 5th node into a 4-node FTT1 cluster
using the real expansion code path (planner + orchestrator + executor +
recreate_lvstore_on_sec etc.) against a real FoundationDB and a simulated
SPDK fleet.

A passing test here validates the entire scaffolding end-to-end:
* real FDB writes/reads work
* the RPC patch routes SPDK calls to per-node simulators
* the orchestrator drives the plan to completion
* the final cluster layout is correct (per the rotation invariants)
"""

import pytest

# Pytest discovers this file via tests/expansion_sim/__init__.py;
# fixtures come from conftest.py in the same directory.


def _expected_layout_4_to_5_ftt1():
    """Expected post-expansion (k=5) layout: secondary(LVS_i) = node[i mod 5].

    Returns a dict {primary_node_id: secondary_node_id}.
    """
    nodes = ["n1", "n2", "n3", "n4", "n5"]
    return {nodes[i]: nodes[(i + 1) % 5] for i in range(5)}


def test_4_to_5_ftt1_happy_path(fdb_clean, db_controller_singleton_reset,
                                 patched_rpc_router):
    """Phase 1 acceptance: full expand from 4 → 5 with FTT=1 succeeds."""
    from tests.expansion_sim._scenarios import build_4_node_ftt1_baseline
    from simplyblock_core.cluster_expand_executor import (
        integrate_new_node_into_cluster,
    )
    from simplyblock_core.cluster_expand_planner import (
        EXPAND_PHASE_COMPLETED,
    )
    from simplyblock_core.db_controller import DBController

    db = DBController()
    cluster_id, _existing_ids, new_node_id = build_4_node_ftt1_baseline(
        patched_rpc_router, db.kv_store)

    cluster = db.get_cluster_by_id(cluster_id)
    new_snode = db.get_storage_node_by_id(new_node_id)

    integrate_new_node_into_cluster(
        cluster, new_snode, db_controller=db,
        manage_cluster_status=False)

    # 1. Plan reached completion.
    cluster = db.get_cluster_by_id(cluster_id)
    assert cluster.expand_state["phase"] == EXPAND_PHASE_COMPLETED, (
        f"expand_state phase = {cluster.expand_state.get('phase')!r}; "
        f"abort_reason = {cluster.expand_state.get('abort_reason')!r}; "
        f"cursor = {cluster.expand_state.get('cursor')}/"
        f"{len(cluster.expand_state.get('moves', []))}")
    assert cluster.expand_state["new_node_id"] == "n5"

    # 2. Newcomer now hosts a primary lvstore.
    n5 = db.get_storage_node_by_id("n5")
    assert n5.lvstore, f"n5 should have a primary lvstore; got {n5.lvstore!r}"
    assert n5.lvstore_status == "ready"

    # 3. Final rotation matches the modulus-5 expectation.
    expected = _expected_layout_4_to_5_ftt1()
    nodes = {nid: db.get_storage_node_by_id(nid) for nid in expected}
    for primary_id, expected_sec_id in expected.items():
        actual = nodes[primary_id].secondary_node_id
        assert actual == expected_sec_id, (
            f"LVS@{primary_id}: expected sec_1 on {expected_sec_id}, "
            f"got {actual!r}")

    # 4. Each LVS's secondary holder records the back-reference.
    for primary_id, sec_id in expected.items():
        sec_node = nodes[sec_id]
        # The sec node's lvstore_stack_secondary_1 should match the
        # primary it hosts, except some nodes host multiple secs after
        # rotation — we only check the at-least-one invariant per
        # secondary node.
        # In the 4→5 layout, every node is sec_1 for exactly one primary,
        # so equality holds.
        assert sec_node.lvstore_stack_secondary_1 == primary_id, (
            f"node {sec_id} lvstore_stack_secondary_1 expected "
            f"{primary_id}, got {sec_node.lvstore_stack_secondary_1!r}")

    # 5. Simulator-side sanity: n5's simulator now holds a lvstore bdev
    # plus the primary stack bdevs.
    n5_sim = patched_rpc_router.get_by_node_id("n5")
    assert any(b.type == "bdev_lvstore" for b in n5_sim.bdevs.values()), \
        f"n5 sim has no lvstore bdev; bdevs={list(n5_sim.bdevs)}"
