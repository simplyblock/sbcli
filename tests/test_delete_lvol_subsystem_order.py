# coding=utf-8
"""
test_delete_lvol_subsystem_order.py — pins the pre-leader subsystem-teardown
contract in ``lvol_controller.delete_lvol`` for HA lvols.

Spec (from PR #1050 review):
1. Fixed role order tertiary -> secondary -> primary.
2. Skip any role whose node status is not STATUS_ONLINE
   (down / in_restart / unreachable / etc).
3. Exactly ONE drain sleep, placed immediately after the primary's
   subsystem delete and BEFORE execute_on_leader_with_failover (which
   targets the *current* leader — not necessarily the primary). The
   drain window was shortened from 2s to 1s in PR #1078; the contract
   that matters is "exactly one sleep, gated on the primary teardown,
   sitting between it and the leader op".
4. If the primary's subsystem delete is skipped (primary not online),
   no sleep is performed.

The incident motivating this contract is the 2026-05-17 dual-outage
soak (logs-incident-20260517-2110/ANALYSIS.md): without an explicit
drain window, the leader's bdev stack disappeared while multipath
clients were still routed at the leader, contributing to the cascade
that took the cluster down.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode

from tests._mocks import make_mock_cluster


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_lvol():
    lvol = LVol()
    lvol.uuid = "lvol-uuid-1"
    lvol.lvol_name = "test_lvol"
    lvol.lvol_bdev = "LVOL_1"
    lvol.lvs_name = "LVS_1"
    lvol.node_id = "node-primary"
    lvol.nodes = ["node-primary", "node-secondary", "node-tertiary"]
    lvol.ha_type = "ha"
    lvol.status = LVol.STATUS_ONLINE
    lvol.pool_uuid = "pool-1"
    lvol.bdev_stack = []
    lvol.nqn = "nqn.test:lvol-uuid-1"
    lvol.ns_id = 1
    lvol.cloned_from_snap = ""
    lvol.delete_snap_on_lvol_delete = False
    return lvol


def _make_pool():
    from simplyblock_core.models.pool import Pool
    p = Pool()
    p.uuid = "pool-1"
    p.status = Pool.STATUS_ACTIVE
    return p


def _make_node(uuid, status=StorageNode.STATUS_ONLINE,
               secondary_id="node-secondary",
               tertiary_id="node-tertiary"):
    """Build a MagicMock StorageNode whose rpc_client returns a fresh
    MagicMock per call (so each call site has its own subsystem_list /
    subsystem_delete capture).

    The rpc_client mock returned by ``node.rpc_client()`` is recorded on
    the node as ``node.rpc_client_mock`` so the test can inspect calls.
    """
    n = MagicMock(spec=StorageNode)
    n.uuid = uuid
    n.status = status
    n.lvstore = "LVS_1"
    n.lvstore_status = "ready"
    n.tertiary_node_id = tertiary_id
    n.secondary_node_id = secondary_id
    n.get_id.return_value = uuid

    rpc_mock = MagicMock()
    # Default: subsystem exists with one namespace whose uuid matches the
    # lvol -> _remove_lvol_subsys_from_node removes the ns, the list then
    # becomes empty, and the whole subsystem is deleted (subsystem_delete
    # path). The namespace dict mirrors the real subsystem_list shape
    # (nsid + bdev_name + uuid) that the production code now matches on.
    rpc_mock.subsystem_list.side_effect = [
        [{"namespaces": [{"nsid": 1, "bdev_name": "LVOL_1",
                          "uuid": "lvol-uuid-1"}]}],
        [{"namespaces": []}],
    ]
    rpc_mock.subsystem_delete.return_value = True
    rpc_mock.nvmf_subsystem_remove_ns.return_value = True
    n.rpc_client_mock = rpc_mock
    n.rpc_client.return_value = rpc_mock
    return n


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSubsystemDeleteOrder(unittest.TestCase):

    def _patch(self, primary_status=StorageNode.STATUS_ONLINE,
               secondary_status=StorageNode.STATUS_ONLINE,
               tertiary_status=StorageNode.STATUS_ONLINE):
        """Wire the DBController, time.sleep, and
        execute_on_leader_with_failover so we can record the call
        sequence across all three node RPCs plus the sleep.
        """
        from simplyblock_core.controllers import lvol_controller as mod

        lvol = _make_lvol()
        lvol.write_to_db = MagicMock()

        primary = _make_node("node-primary", primary_status)
        secondary = _make_node("node-secondary", secondary_status,
                               secondary_id=None, tertiary_id=None)
        tertiary = _make_node("node-tertiary", tertiary_status,
                              secondary_id=None, tertiary_id=None)

        def _get_node(node_id):
            return {
                "node-primary": primary,
                "node-secondary": secondary,
                "node-tertiary": tertiary,
            }[node_id]

        db_mock = MagicMock()
        db_mock.get_lvol_by_id.return_value = lvol
        db_mock.get_lvol_by_name.return_value = lvol
        db_mock.get_storage_node_by_id.side_effect = _get_node
        db_mock.get_pool_by_id.return_value = _make_pool()
        db_mock.get_cluster_by_id.return_value = make_mock_cluster()
        db_mock.kv_store = MagicMock()

        from simplyblock_core.controllers import migration_controller as _mig
        mig_patch = patch.object(_mig, "get_active_migration_for_lvol",
                                 return_value=None)

        # Shared call recorder. We append one entry per recorded action so
        # we can assert tertiary -> secondary -> primary -> SLEEP -> leader_op
        # all in one ordered list.
        events = []

        # Wrap each node's subsystem_delete / nvmf_subsystem_remove_ns to
        # record events. subsystem_list is not recorded — only the actual
        # teardown call is interesting for the order assertion.
        def _wrap_subsys_delete(node_uuid, rpc_mock):
            orig = rpc_mock.subsystem_delete
            def _record(nqn):
                events.append(("subsystem_delete", node_uuid))
                return orig(nqn)
            rpc_mock.subsystem_delete = MagicMock(side_effect=_record)

        for n in (primary, secondary, tertiary):
            _wrap_subsys_delete(n.uuid, n.rpc_client_mock)

        # time.sleep records a SLEEP event.
        def _record_sleep(seconds):
            events.append(("sleep", seconds))

        # execute_on_leader_with_failover records LEADER_OP then returns success.
        def _exec(all_nodes, lvs_name, fn):
            events.append(("leader_op", all_nodes[0].get_id()))
            return (True, all_nodes[0], True)

        # delete_lvol_from_node would otherwise try real RPC; replace it
        # with a stub. The leader subsystem deletion has already happened
        # in our pre-leader phase (subsystem_list mocked to empty for the
        # second call would be more realistic, but the helper is now
        # idempotent and a True return is enough).
        del_from_node_patch = patch.object(mod, "delete_lvol_from_node",
                                           return_value=True)

        check_mock = MagicMock(return_value="skip")

        self._patches = [
            patch.object(mod, "DBController", return_value=db_mock),
            mig_patch,
            patch.object(mod.time, "sleep", side_effect=_record_sleep),
            patch("simplyblock_core.storage_node_ops.execute_on_leader_with_failover",
                  side_effect=_exec),
            patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                  check_mock),
            patch("simplyblock_core.storage_node_ops.queue_for_restart_drain",
                  MagicMock()),
            del_from_node_patch,
        ]
        for p in self._patches:
            p.start()

        return mod, events, primary, secondary, tertiary

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()

    def test_all_online_order_tertiary_secondary_primary_sleep_leader(self):
        """All three peers online: order must be T -> S -> P -> sleep(2) -> leader_op."""
        mod, events, _p, _s, _t = self._patch()

        ok = mod.delete_lvol("lvol-uuid-1", force_delete=False)
        self.assertTrue(ok)

        # Filter out anything other than the four signal events. (The
        # leader_op stub doesn't run delete_lvol_from_node, so there are
        # no extra subsystem_delete events.)
        self.assertEqual(events, [
            ("subsystem_delete", "node-tertiary"),
            ("subsystem_delete", "node-secondary"),
            ("subsystem_delete", "node-primary"),
            ("sleep", 1),
            ("leader_op", "node-primary"),
        ])

    def test_tertiary_offline_skipped(self):
        """If tertiary is not ONLINE its subsystem delete is skipped, but
        secondary + primary still run, sleep still fires after primary."""
        mod, events, _p, _s, _t = self._patch(
            tertiary_status=StorageNode.STATUS_DOWN)

        ok = mod.delete_lvol("lvol-uuid-1", force_delete=False)
        self.assertTrue(ok)

        self.assertEqual(events, [
            ("subsystem_delete", "node-secondary"),
            ("subsystem_delete", "node-primary"),
            ("sleep", 1),
            ("leader_op", "node-primary"),
        ])

    def test_secondary_in_restart_skipped(self):
        """in_restart status is a skip case per the spec."""
        mod, events, _p, _s, _t = self._patch(
            secondary_status=StorageNode.STATUS_RESTARTING)

        ok = mod.delete_lvol("lvol-uuid-1", force_delete=False)
        self.assertTrue(ok)

        self.assertEqual(events, [
            ("subsystem_delete", "node-tertiary"),
            ("subsystem_delete", "node-primary"),
            ("sleep", 1),
            ("leader_op", "node-primary"),
        ])

    def test_primary_offline_no_sleep(self):
        """If the primary is not ONLINE we skip its subsystem delete AND
        the sleep — there is nothing to drain on a node that's already
        gone."""
        mod, events, _p, _s, _t = self._patch(
            primary_status=StorageNode.STATUS_UNREACHABLE)

        ok = mod.delete_lvol("lvol-uuid-1", force_delete=False)
        self.assertTrue(ok)

        # No sleep event, no primary subsystem delete.
        self.assertEqual(events, [
            ("subsystem_delete", "node-tertiary"),
            ("subsystem_delete", "node-secondary"),
            ("leader_op", "node-primary"),
        ])

    def test_only_one_sleep_total(self):
        """Even with all three peers online, there must be exactly one
        drain sleep and it must land immediately before the leader op."""
        mod, events, _p, _s, _t = self._patch()
        mod.delete_lvol("lvol-uuid-1", force_delete=False)

        sleeps = [e for e in events if e[0] == "sleep"]
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(sleeps[0], ("sleep", 1))

        # And it sits exactly between the primary teardown and the leader op.
        sleep_idx = events.index(("sleep", 1))
        self.assertEqual(events[sleep_idx - 1], ("subsystem_delete", "node-primary"))
        self.assertEqual(events[sleep_idx + 1][0], "leader_op")

    def test_rpc_called_with_short_timeout_and_retry(self):
        """The pre-leader rpc_client must be built with (timeout=5, retry=2)
        so a hung peer doesn't block the user-facing delete request."""
        mod, _events, primary, secondary, tertiary = self._patch()
        mod.delete_lvol("lvol-uuid-1", force_delete=False)

        for n in (primary, secondary, tertiary):
            n.rpc_client.assert_called_with(timeout=5, retry=2)


if __name__ == "__main__":
    unittest.main()
