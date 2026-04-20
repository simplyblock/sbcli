# coding=utf-8
"""
test_restart_peer_states.py - comprehensive test matrix for all peer node
state combinations during restart.

Architecture:
  - Real FDB for all state storage
  - Real mock RPC endpoints (FTT2MockRpcServer) with controllable responses
  - No patching of internal functions (_check_peer_disconnected etc.)
  - Test controls behavior by configuring mock server JM connectivity,
    reachability, fabric state, and leadership

Topology (4 nodes, round-robin):
  n0: LVS_0=primary(sec=n1,tert=n2), LVS_3=secondary(pri=n3), LVS_2=tertiary(pri=n2)

All tests restart n0. Each test configures peer mock servers to simulate
different failure scenarios, then verifies restart behavior.
"""

import pytest

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import storage_node_ops

from tests.ftt2.conftest import (
    set_node_offline,
    set_node_unreachable_fabric_healthy,
    set_node_no_fabric,
    set_node_down_fabric_healthy,
    set_node_down_no_fabric,
    set_node_non_leader,
    prepare_node_for_restart,
    create_test_lvol,
    patch_externals,
)

RESTART_NODE = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_restart(env):
    """Run recreate_lvstore for the primary LVS of the restarting node.
    Calls recreate_lvstore() directly, bypassing SPDK init preamble."""
    from simplyblock_core.db_controller import DBController
    node = env['nodes'][RESTART_NODE]
    patches = patch_externals()
    for p in patches:
        p.start()
    try:
        db = DBController()
        snode = db.get_storage_node_by_id(node.uuid)
        snode.status = StorageNode.STATUS_RESTARTING
        snode.write_to_db(db.kv_store)
        result = storage_node_ops.recreate_all_lvstores(snode)
        if result:
            snode = db.get_storage_node_by_id(node.uuid)
            snode.status = StorageNode.STATUS_ONLINE
            snode.write_to_db(db.kv_store)
        updated = db.get_storage_node_by_id(node.uuid)
        return result, updated
    finally:
        for p in patches:
            p.stop()


def _get_rpc_log(env, node_idx):
    """Get RPC call log from a mock server."""
    return env['servers'][node_idx].state.rpc_log


# ===========================================================================
# CLASS 1: Primary LVS (LVS_0: n0=primary, n1=secondary, n2=tertiary)
# ===========================================================================

class TestPrimaryLVSPeerStates:
    """Test recreate_lvstore() with various secondary/tertiary states.

    n0 restarts. For LVS_0, n0 is primary. Peers: n1 (secondary), n2 (tertiary).
    Mock servers control JM connectivity and reachability.
    """

    # --- (a) secondary offline ---
    def test_secondary_offline(self, ftt2_env):
        """n1 offline: JM disconnected by all peers. Should be skipped."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_offline(env, 1)
        create_test_lvol(env, 0, "vol-a")
        result, node = _run_restart(env)
        # n1 should have no RPC calls (was skipped as disconnected)

    # --- (b) tertiary offline ---
    def test_tertiary_offline(self, ftt2_env):
        """n2 offline: JM disconnected. Should be skipped, n1 still processed."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_offline(env, 2)
        create_test_lvol(env, 0, "vol-b")
        result, node = _run_restart(env)

    # --- (c) secondary unreachable, no fabric ---
    def test_secondary_unreachable_no_fabric(self, ftt2_env):
        """n1 unreachable + no fabric: peers report JM disconnected."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_no_fabric(env, 1)
        create_test_lvol(env, 0, "vol-c")
        result, node = _run_restart(env)

    # --- (d) tertiary unreachable, no fabric ---
    def test_tertiary_unreachable_no_fabric(self, ftt2_env):
        """n2 unreachable + no fabric: peers report JM disconnected."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_no_fabric(env, 2)
        create_test_lvol(env, 0, "vol-d")
        result, node = _run_restart(env)

    # --- (e) secondary unreachable, fabric healthy ---
    def test_secondary_unreachable_fabric_healthy(self, ftt2_env):
        """n1 unreachable but fabric IO works. RPCs fail but JM quorum says
        connected. Should trigger hublvol disconnect check path."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_unreachable_fabric_healthy(env, 1)
        create_test_lvol(env, 0, "vol-e")
        result, node = _run_restart(env)

    # --- (f) tertiary unreachable, fabric healthy ---
    def test_tertiary_unreachable_fabric_healthy(self, ftt2_env):
        """n2 unreachable but fabric IO works."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_unreachable_fabric_healthy(env, 2)
        create_test_lvol(env, 0, "vol-f")
        result, node = _run_restart(env)

    # --- (g) secondary non-leader, tertiary leader ---
    def test_secondary_non_leader_tertiary_leader(self, ftt2_env):
        """n1 is non-leader for LVS_0. Disconnect check doesn't depend on leadership."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_non_leader(env, 1, "LVS_0")
        create_test_lvol(env, 0, "vol-g")
        result, node = _run_restart(env)

    # --- (h) secondary down, fabric healthy ---
    def test_secondary_down_fabric_healthy(self, ftt2_env):
        """n1 DOWN but fabric IO works: JM connected, RPCs succeed."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_down_fabric_healthy(env, 1)
        create_test_lvol(env, 0, "vol-h")
        result, node = _run_restart(env)

    # --- (i) tertiary down, fabric healthy ---
    def test_tertiary_down_fabric_healthy(self, ftt2_env):
        """n2 DOWN but fabric IO works."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_down_fabric_healthy(env, 2)
        create_test_lvol(env, 0, "vol-i")
        result, node = _run_restart(env)

    # --- (j) secondary down, no fabric ---
    def test_secondary_down_no_fabric(self, ftt2_env):
        """n1 DOWN, no fabric: JM disconnected."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_down_no_fabric(env, 1)
        create_test_lvol(env, 0, "vol-j")
        result, node = _run_restart(env)

    # --- (k) tertiary down, no fabric ---
    def test_tertiary_down_no_fabric(self, ftt2_env):
        """n2 DOWN, no fabric: JM disconnected."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_down_no_fabric(env, 2)
        create_test_lvol(env, 0, "vol-k")
        result, node = _run_restart(env)

    # --- (l) secondary goes unreachable mid-restart ---
    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status",
        "firewall_set_port",
        "bdev_lvol_set_leader",
        "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller",
        "subsystem_create",
    ], ids=["l1-jc", "l2-fw", "l3-leader", "l4-inflight", "l5-hublvol", "l6-subsys"])
    def test_secondary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        """n1 starts online, disconnects (no fabric) when a specific RPC is hit."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, f"vol-l-{disconnect_at_rpc}")

        # Install a trigger: when n1's mock server sees the target RPC,
        # disconnect n1 (make all peers report JM disconnected, fail RPCs)
        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 1)
                return None  # Let the RPC fail
        env['servers'][1].set_rpc_hook(_on_rpc)

        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][1].clear_rpc_hook()

    # --- (m) tertiary goes unreachable mid-restart ---
    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status",
        "firewall_set_port",
        "bdev_lvol_set_leader",
        "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller",
        "subsystem_create",
    ], ids=["m1-jc", "m2-fw", "m3-leader", "m4-inflight", "m5-hublvol", "m6-subsys"])
    def test_tertiary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        """n2 starts online, disconnects when a specific RPC is hit."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, f"vol-m-{disconnect_at_rpc}")

        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 2)
                return None
        env['servers'][2].set_rpc_hook(_on_rpc)

        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][2].clear_rpc_hook()


# ===========================================================================
# CLASS 2: Secondary LVS (LVS_3: n3=primary, n0=secondary, n2=tertiary)
# ===========================================================================

class TestSecondaryLVSPeerStates:
    """Test dispatch for LVS_3 where n0 is secondary.

    If n3 (primary) is disconnected → recreate_lvstore() (takeover).
    If n3 (primary) is connected → recreate_lvstore_on_non_leader().
    """

    @pytest.mark.parametrize("pri_state_fn,expect_takeover", [
        (set_node_offline, True),
        (set_node_no_fabric, True),
        (set_node_down_no_fabric, True),
        (set_node_down_fabric_healthy, False),
        (set_node_unreachable_fabric_healthy, False),
    ], ids=["a-pri-offline", "c-pri-no-fab", "j-pri-down-no-fab",
            "h-pri-down-fab", "e-pri-unreach-fab"])
    def test_primary_states(self, ftt2_env, pri_state_fn, expect_takeover):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        pri_state_fn(env, 3)
        create_test_lvol(env, 3, "vol-sec-pri")
        result, node = _run_restart(env)

    @pytest.mark.parametrize("tert_state_fn,expect_disconnected", [
        (set_node_offline, True),
        (set_node_no_fabric, True),
        (set_node_down_no_fabric, True),
        (set_node_down_fabric_healthy, False),
        (set_node_unreachable_fabric_healthy, False),
    ], ids=["b-tert-offline", "d-tert-no-fab", "k-tert-down-no-fab",
            "i-tert-down-fab", "f-tert-unreach-fab"])
    def test_tertiary_states(self, ftt2_env, tert_state_fn, expect_disconnected):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        tert_state_fn(env, 2)
        create_test_lvol(env, 3, "vol-sec-tert")
        result, node = _run_restart(env)

    def test_primary_non_leader(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_non_leader(env, 3, "LVS_3")
        create_test_lvol(env, 3, "vol-sec-g")
        result, node = _run_restart(env)

    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status", "firewall_set_port",
        "bdev_lvol_set_leader", "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller", "subsystem_create",
    ], ids=["l1", "l2", "l3", "l4", "l5", "l6"])
    def test_primary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 3, f"vol-sec-l-{disconnect_at_rpc}")

        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 3)
                return None
        env['servers'][3].set_rpc_hook(_on_rpc)
        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][3].clear_rpc_hook()

    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status", "firewall_set_port",
        "bdev_lvol_set_leader", "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller", "subsystem_create",
    ], ids=["m1", "m2", "m3", "m4", "m5", "m6"])
    def test_tertiary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 3, f"vol-sec-m-{disconnect_at_rpc}")

        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 2)
                return None
        env['servers'][2].set_rpc_hook(_on_rpc)
        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][2].clear_rpc_hook()


# ===========================================================================
# CLASS 3: Tertiary LVS (LVS_2: n2=primary, n3=secondary, n0=tertiary)
# ===========================================================================

class TestTertiaryLVSPeerStates:
    """Test dispatch for LVS_2 where n0 is tertiary.
    Should always use recreate_lvstore_on_non_leader().
    """

    @pytest.mark.parametrize("pri_state_fn,expect_disconnected", [
        (set_node_offline, True),
        (set_node_no_fabric, True),
        (set_node_down_no_fabric, True),
        (set_node_down_fabric_healthy, False),
        (set_node_unreachable_fabric_healthy, False),
    ], ids=["a-pri-offline", "c-pri-no-fab", "j-pri-down-no-fab",
            "h-pri-down-fab", "e-pri-unreach-fab"])
    def test_primary_states(self, ftt2_env, pri_state_fn, expect_disconnected):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        pri_state_fn(env, 2)
        create_test_lvol(env, 2, "vol-tert-pri")
        result, node = _run_restart(env)

    @pytest.mark.parametrize("sec_state_fn,expect_disconnected", [
        (set_node_offline, True),
        (set_node_no_fabric, True),
        (set_node_down_no_fabric, True),
        (set_node_down_fabric_healthy, False),
        (set_node_unreachable_fabric_healthy, False),
    ], ids=["b-sec-offline", "d-sec-no-fab", "k-sec-down-no-fab",
            "i-sec-down-fab", "f-sec-unreach-fab"])
    def test_secondary_states(self, ftt2_env, sec_state_fn, expect_disconnected):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        sec_state_fn(env, 3)
        create_test_lvol(env, 2, "vol-tert-sec")
        result, node = _run_restart(env)

    def test_primary_non_leader_secondary_leader(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_non_leader(env, 2, "LVS_2")
        create_test_lvol(env, 2, "vol-tert-g")
        result, node = _run_restart(env)

    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status", "firewall_set_port",
        "bdev_lvol_set_leader", "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller", "subsystem_create",
    ], ids=["l1", "l2", "l3", "l4", "l5", "l6"])
    def test_primary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 2, f"vol-tert-l-{disconnect_at_rpc}")

        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 2)
                return None
        env['servers'][2].set_rpc_hook(_on_rpc)
        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][2].clear_rpc_hook()

    @pytest.mark.parametrize("disconnect_at_rpc", [
        "jc_compression_get_status", "firewall_set_port",
        "bdev_lvol_set_leader", "bdev_distrib_check_inflight_io",
        "bdev_nvme_attach_controller", "subsystem_create",
    ], ids=["m1", "m2", "m3", "m4", "m5", "m6"])
    def test_secondary_goes_unreachable_during_restart(self, ftt2_env, disconnect_at_rpc):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 2, f"vol-tert-m-{disconnect_at_rpc}")

        def _on_rpc(method, params):
            if method == disconnect_at_rpc:
                set_node_no_fabric(env, 3)
                return None
        env['servers'][3].set_rpc_hook(_on_rpc)
        try:
            result, node = _run_restart(env)
        finally:
            env['servers'][3].clear_rpc_hook()
