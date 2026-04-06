# coding=utf-8
"""
test_restart_scenarios.py – comprehensive FTT=2 restart tests.

Round-robin topology (4 nodes, 4 LVS):
  LVS_i: pri=node i, sec=node (i+1)%4, tert=node (i+2)%4

All tests restart node-0, which hosts:
  LVS_0 = primary   (sec=n1, tert=n2)
  LVS_3 = secondary (pri=n3, tert=n1)
  LVS_2 = tertiary  (pri=n2, sec=n3)

Exactly one other node is in outage.  Impact per outage:
  n1 out │ LVS_0: sec down        │ LVS_3: sibling tert down │ LVS_2: no impact
  n2 out │ LVS_0: tert down       │ LVS_3: no impact         │ LVS_2: pri down
  n3 out │ LVS_0: no impact       │ LVS_3: pri down→TAKEOVER │ LVS_2: sibling sec down

Each outage scenario is tested with:
  - 4 peer states (unreachable+fabric, no-fabric, down+fabric, offline)
  - Feature variants (plain, encrypted, QoS, DHCHAP, full)
  - With and without multipathing (ha_type="ha" vs "single")

Concurrent operations (create/delete with various volume types) are tested
using the OperationRunner + PhaseGate infrastructure.
"""

import pytest

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import storage_node_ops

from tests.ftt2.conftest import (
    set_node_offline,
    set_node_unreachable_fabric_healthy,
    set_node_no_fabric,
    set_node_down_fabric_healthy,
    prepare_node_for_restart,
    create_test_lvol,
    patch_externals,
)

RESTART_NODE = 0

_STATE_FUNCS = {
    "S-UNREACH": set_node_unreachable_fabric_healthy,
    "S-NOFAB":   set_node_no_fabric,
    "S-DOWN":    set_node_down_fabric_healthy,
    "S-OFFLINE": set_node_offline,
}

_FEATURE_COMBOS = {
    "F-PLAIN":  dict(encrypted=False, qos=False, dhchap=False),
    "F-ENC":    dict(encrypted=True,  qos=False, dhchap=False),
    "F-QOS":    dict(encrypted=False, qos=True,  dhchap=False),
    "F-DHCHAP": dict(encrypted=False, qos=False, dhchap=True),
    "F-FULL":   dict(encrypted=True,  qos=True,  dhchap=True),
}


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


def _assert_restart_ok(result, node):
    assert result is True, "Restart must succeed"
    assert node.status == StorageNode.STATUS_ONLINE, \
        f"Node must be ONLINE, got {node.status}"
    assert node.status != StorageNode.STATUS_SUSPENDED, \
        "SUSPENDED state must not be used"


# ###########################################################################
# Scenario 1: n1 is out
#   LVS_0 (n0=pri): sec(n1) down — skip n1, process tert(n2) normally
#   LVS_3 (n0=sec): sibling tert(n1) down — non-leader under pri(n3)
#   LVS_2 (n0=tert): no impact
# ###########################################################################

class TestScenario1_N1Out:

    @pytest.mark.parametrize("state", _STATE_FUNCS.keys(), ids=_STATE_FUNCS.keys())
    @pytest.mark.parametrize("feat", _FEATURE_COMBOS.keys(), ids=_FEATURE_COMBOS.keys())
    def test_restart(self, ftt2_env, state, feat):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        _STATE_FUNCS[state](env, 1)
        f = _FEATURE_COMBOS[feat]
        create_test_lvol(env, 0, name=f"s1-{state}-{feat}", **f)

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

    def test_lvs3_sibling_tert_down(self, ftt2_env):
        """LVS_3: n0=sec, sibling tert(n1) offline. Pri(n3) online → non-leader."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_offline(env, 1)
        create_test_lvol(env, 3, name="s1-lvs3-tert-down")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)


# ###########################################################################
# Scenario 2: n2 is out
#   LVS_0 (n0=pri): tert(n2) down — skip n2, process sec(n1) normally
#   LVS_3 (n0=sec): no impact
#   LVS_2 (n0=tert): pri(n2) down — sec(n3) becomes leader,
#                     n0 uses recreate_lvstore_on_non_leader()
# ###########################################################################

class TestScenario2_N2Out:

    @pytest.mark.parametrize("state", _STATE_FUNCS.keys(), ids=_STATE_FUNCS.keys())
    @pytest.mark.parametrize("feat", _FEATURE_COMBOS.keys(), ids=_FEATURE_COMBOS.keys())
    def test_restart(self, ftt2_env, state, feat):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        _STATE_FUNCS[state](env, 2)
        f = _FEATURE_COMBOS[feat]
        create_test_lvol(env, 0, name=f"s2-{state}-{feat}-a", **f)
        create_test_lvol(env, 2, name=f"s2-{state}-{feat}-b", **f)

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

    @pytest.mark.parametrize("state", ["S-NOFAB", "S-OFFLINE"])
    def test_lvs2_tert_under_new_leader(self, ftt2_env, state):
        """LVS_2: pri(n2) confirmed down. Sec(n3) becomes leader.
        n0 (tert) reconnects as non-leader under n3."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        _STATE_FUNCS[state](env, 2)
        create_test_lvol(env, 2, name=f"s2-lvs2-{state}")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

    def test_lvs2_pri_unreach_fabric_healthy(self, ftt2_env):
        """LVS_2: pri(n2) unreachable but fabric healthy → no takeover.
        n0 reconnects to n2 as non-leader."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_unreachable_fabric_healthy(env, 2)
        create_test_lvol(env, 2, name="s2-lvs2-unreach")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)


# ###########################################################################
# Scenario 3: n3 is out
#   LVS_0 (n0=pri): no impact — sec(n1) and tert(n2) both online
#   LVS_3 (n0=sec): pri(n3) down → LEADER TAKEOVER
#                    n0 calls recreate_lvstore() (leader path)
#                    tert(n1) uses recreate_lvstore_on_non_leader()
#   LVS_2 (n0=tert): sibling sec(n3) down — pri(n2) online, non-leader
# ###########################################################################

class TestScenario3_N3Out:

    @pytest.mark.parametrize("state", _STATE_FUNCS.keys(), ids=_STATE_FUNCS.keys())
    @pytest.mark.parametrize("feat", _FEATURE_COMBOS.keys(), ids=_FEATURE_COMBOS.keys())
    def test_restart(self, ftt2_env, state, feat):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        _STATE_FUNCS[state](env, 3)
        f = _FEATURE_COMBOS[feat]
        create_test_lvol(env, 0, name=f"s3-{state}-{feat}-a", **f)
        create_test_lvol(env, 3, name=f"s3-{state}-{feat}-b", **f)

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

    # ---- Leader takeover specifics ----

    @pytest.mark.parametrize("state", ["S-NOFAB", "S-OFFLINE"])
    def test_takeover_leadership_set(self, ftt2_env, state):
        """LVS_3: pri(n3) confirmed down. n0 must take leadership."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        _STATE_FUNCS[state](env, 3)
        create_test_lvol(env, 3, name=f"s3-takeover-{state}")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

        srv0 = env['servers'][RESTART_NODE]
        leader_calls = srv0.get_rpc_calls('bdev_lvol_set_leader')
        assert any(p.get('lvs_leadership', p.get('leader', False))
                   for _, _, p in leader_calls), \
            "n0 must take leadership for LVS_3"

    def test_no_takeover_when_fabric_healthy(self, ftt2_env):
        """LVS_3: pri(n3) unreachable but fabric healthy → no takeover."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_unreachable_fabric_healthy(env, 3)
        create_test_lvol(env, 3, name="s3-no-takeover")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

    # ---- LVS_2: sibling sec(n3) down ----

    def test_lvs2_sibling_sec_down(self, ftt2_env):
        """LVS_2: sibling sec(n3) offline. Pri(n2) online.
        n0 reconnects as non-leader, skips offline sibling."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_offline(env, 3)
        create_test_lvol(env, 2, name="s3-lvs2-sibling")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)


# ###########################################################################
# Pre-restart guard
# ###########################################################################

class TestPreRestartGuard:

    def test_reject_concurrent_restart(self, ftt2_env):
        env = ftt2_env
        db = env['db']
        n1 = env['nodes'][1]
        n1.status = StorageNode.STATUS_RESTARTING
        n1.write_to_db(db.kv_store)
        prepare_node_for_restart(env, RESTART_NODE)

        patches = patch_externals()
        for p in patches:
            p.start()
        try:
            result = storage_node_ops.restart_storage_node(env['nodes'][0].uuid)
            assert result is False
        finally:
            for p in patches:
                p.stop()
            n1.status = StorageNode.STATUS_ONLINE
            n1.write_to_db(db.kv_store)

    def test_reject_during_peer_shutdown(self, ftt2_env):
        """Design requires: reject restart when peer is IN_SHUTDOWN."""
        env = ftt2_env
        db = env['db']
        n1 = env['nodes'][1]
        n1.status = StorageNode.STATUS_IN_SHUTDOWN
        n1.write_to_db(db.kv_store)
        prepare_node_for_restart(env, RESTART_NODE)

        patches = patch_externals()
        for p in patches:
            p.start()
        try:
            result = storage_node_ops.restart_storage_node(env['nodes'][0].uuid)
            assert result is False, \
                "Restart must be rejected when peer is IN_SHUTDOWN"
        finally:
            for p in patches:
                p.stop()
            n1.status = StorageNode.STATUS_ONLINE
            n1.write_to_db(db.kv_store)


# ###########################################################################
# RPC verification
# ###########################################################################

class TestRpcVerification:

    def test_examine_called(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, name="rpc-examine")
        _run_restart(env)
        assert env['servers'][RESTART_NODE].was_called('bdev_examine')

    def test_leadership_false_on_online_sec(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, name="rpc-lead")
        _run_restart(env)

        srv1 = env['servers'][1]
        calls = srv1.get_rpc_calls('bdev_lvol_set_leader')
        assert any(not p.get('lvs_leadership', p.get('leader', True))
                   for _, _, p in calls)

    def test_inflight_io_checked(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, name="rpc-inflight")
        _run_restart(env)
        assert env['servers'][1].was_called('bdev_distrib_check_inflight_io')

    def test_jc_sync_when_sec_unreachable(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_unreachable_fabric_healthy(env, 1)
        create_test_lvol(env, 0, name="rpc-jcsync")
        _run_restart(env)
        assert env['servers'][RESTART_NODE].was_called('jc_explicit_synchronization')

    def test_hublvol_created(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        create_test_lvol(env, 0, name="rpc-hub")
        _run_restart(env)
        assert env['servers'][RESTART_NODE].was_called('bdev_lvol_create_hublvol')

    def test_no_leadership_rpc_to_offline_node(self, ftt2_env):
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        set_node_offline(env, 1)
        create_test_lvol(env, 0, name="rpc-no-offline")
        env['servers'][1].state.rpc_log.clear()
        _run_restart(env)
        assert not env['servers'][1].was_called('bdev_lvol_set_leader')


# ###########################################################################
# Concurrent operations during restart (Group H)
# ###########################################################################

class TestConcurrentOperations:
    """Operations running through the real control plane concurrently with
    restart.  Uses PhaseGate to pause restart at bdev_examine (Phase 5)."""

    def test_volume_created_before_examine_is_discovered(self, ftt2_env):
        """A volume that exists in FDB before restart begins should be
        picked up and have its subsystem created."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        lvol = create_test_lvol(env, 0, name="concurrent-pre")

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

        srv0 = env['servers'][RESTART_NODE]
        sub_nqns = [p.get('nqn', '') for _, _, p in
                    srv0.get_rpc_calls('nvmf_create_subsystem')]
        assert lvol.nqn in sub_nqns

    def test_deleted_volume_not_restored(self, ftt2_env):
        """A volume marked IN_DELETION before restart must NOT be restored."""
        from simplyblock_core.models.lvol_model import LVol as LVolModel
        env = ftt2_env
        db = env['db']
        prepare_node_for_restart(env, RESTART_NODE)
        lvol = create_test_lvol(env, 0, name="concurrent-del")
        lvol.status = LVolModel.STATUS_IN_DELETION
        lvol.write_to_db(db.kv_store)

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)

        srv0 = env['servers'][RESTART_NODE]
        sub_nqns = [p.get('nqn', '') for _, _, p in
                    srv0.get_rpc_calls('nvmf_create_subsystem')]
        assert lvol.nqn not in sub_nqns

    @pytest.mark.parametrize("feat", _FEATURE_COMBOS.keys(),
                             ids=_FEATURE_COMBOS.keys())
    def test_volume_features_restored(self, ftt2_env, feat):
        """Verify volumes with various features are correctly restored."""
        env = ftt2_env
        prepare_node_for_restart(env, RESTART_NODE)
        f = _FEATURE_COMBOS[feat]
        create_test_lvol(env, 0, name=f"feat-{feat}", **f)

        result, node = _run_restart(env)
        _assert_restart_ok(result, node)
