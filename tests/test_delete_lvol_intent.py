# coding=utf-8
"""
test_delete_lvol_intent.py — regression tests for the early-IN_DELETION
persistence in ``simplyblock_core.controllers.lvol_controller.delete_lvol``.

Background: in n+k outage soak run 20260426-164436, a clone delete during
iter 5 (when the secondary + tertiary of LVS_5028 were being shut down)
was issued at 22:47:55 against an online-but-back-pressured leader (.202).
The leader's SPDK was processing a flood of qpair-cleanup events from the
container-killed peer; the 5s/×2 RPC budget in delete_lvol_from_node
expired and the leader op raised. delete_lvol then returned False without
ever writing STATUS_IN_DELETION or deletion_status to FDB. The lvol
stayed 'online'; the API returned ``{"results": False, "status": True}``
(HTTP 200); no background reconciler had any record of the deletion
intent; the test harness's deferred validation never re-issued the
DELETE; the clone became permanently orphaned even though a 4th attempt
~5 minutes later would have succeeded once SPDK settled.

Fix: persist STATUS_IN_DELETION before the data-plane RPC. lvol_monitor's
existing STATUS_IN_DELETION reconcile path then drives the delete to
completion in the background — surviving any number of transient
leader-RPC failures during an outage window without needing the harness
to retry.

These tests pin:
1. After delete_lvol returns, the lvol is in ``STATUS_IN_DELETION`` even
   when the leader op fails.
2. The status write happens BEFORE execute_on_leader_with_failover is
   invoked (call ordering on the DB mock).
3. A second delete_lvol on an already-in-deletion lvol short-circuits
   to True without re-running the leader op (idempotency invariant).
4. Source-level invariant: the IN_DELETION assignment in delete_lvol is
   located before the ha-type branch, not after it.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_lvol(status=LVol.STATUS_ONLINE, ha_type="ha"):
    lvol = LVol()
    lvol.uuid = "lvol-uuid-1"
    lvol.lvol_name = "test_lvol"
    lvol.lvol_bdev = "LVOL_1"
    lvol.lvs_name = "LVS_1"
    lvol.node_id = "node-leader"
    lvol.nodes = ["node-leader", "node-sec", "node-tert"]
    lvol.ha_type = ha_type
    lvol.status = status
    lvol.pool_uuid = "pool-1"
    lvol.bdev_stack = []
    lvol.cloned_from_snap = ""
    lvol.delete_snap_on_lvol_delete = False
    return lvol


def _make_pool():
    from simplyblock_core.models.pool import Pool
    p = Pool()
    p.uuid = "pool-1"
    p.status = Pool.STATUS_ACTIVE
    return p


def _make_node(uuid="node-leader"):
    from simplyblock_core.models.storage_node import StorageNode
    n = MagicMock(spec=StorageNode)
    n.uuid = uuid
    n.lvstore_status = "ready"
    n.get_id.return_value = uuid
    return n


# ---------------------------------------------------------------------------
# Runtime tests
# ---------------------------------------------------------------------------


class TestDeleteLvolIntentPersisted(unittest.TestCase):
    """delete_lvol must persist STATUS_IN_DELETION before any data-plane RPC."""

    def _patch(self, lvol, leader_op_fails=False):
        """Wire up mocks for a single delete_lvol call.

        Returns (mod, db_mock, exec_mock, lvol). The lvol's ``write_to_db``
        is real (mutates self.status); the DB controller is mocked to
        return our lvol. ``execute_on_leader_with_failover`` is patched to
        either succeed or "fail" (mimicking an RPC timeout/error path).
        """
        from simplyblock_core.controllers import lvol_controller as mod

        # ``write_to_db`` on a real LVol would hit FDB; replace it with a
        # noop that records the call so we can assert ordering.
        write_calls = []

        def _record_write(*_a, **_kw):
            # Snapshot the status at call time so ordering is checkable.
            write_calls.append(lvol.status)

        lvol.write_to_db = _record_write  # type: ignore[assignment]

        # DB controller mock
        db_mock = MagicMock()
        db_mock.get_lvol_by_id.return_value = lvol
        db_mock.get_lvol_by_name.return_value = lvol
        db_mock.get_storage_node_by_id.return_value = _make_node()
        db_mock.get_pool_by_id.return_value = _make_pool()
        db_mock.kv_store = MagicMock()

        # execute_on_leader_with_failover mock. Return shape mirrors prod:
        # (success_bool, leader_node_or_None, result_or_message).
        if leader_op_fails:
            exec_return = (False, _make_node(), "Operation failed on leader")
        else:
            exec_return = (True, _make_node(), True)
        exec_mock = MagicMock(return_value=exec_return)

        # check_non_leader_for_operation: skip everything on non-leaders so
        # we don't have to mock RPC clients on secondaries.
        check_mock = MagicMock(return_value="skip")

        # migration_controller is imported lazily *inside* delete_lvol via
        # ``from simplyblock_core.controllers import migration_controller``.
        # Patch the function at its source module so the local import
        # binds to our mock.
        from simplyblock_core.controllers import migration_controller as _mig
        mig_func_patch = patch.object(_mig, "get_active_migration_for_lvol",
                                       return_value=None)

        self._patches = [
            patch.object(mod, "DBController", return_value=db_mock),
            mig_func_patch,
            patch("simplyblock_core.storage_node_ops.execute_on_leader_with_failover", exec_mock),
            patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation", check_mock),
            patch("simplyblock_core.storage_node_ops.queue_for_restart_drain", MagicMock()),
            # delete_lvol_from_node would attempt real RPC on the leader;
            # we don't care about its side effects in these tests since
            # the leader op outcome is controlled by execute_on_leader…
            # mock above. But it's still called from the flow when
            # leader_op_completed paths run, so we patch it.
            patch.object(mod, "delete_lvol_from_node", return_value=True),
        ]
        for p in self._patches:
            p.start()

        return mod, db_mock, exec_mock, write_calls

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()

    def test_status_in_deletion_when_leader_op_fails(self):
        """If the leader op fails, the lvol MUST be left in IN_DELETION
        (not reverted to ONLINE) so lvol_monitor's reconciler can drive
        completion. Pre-fix, the failure path returned without writing
        any status, leaving the lvol orphaned in 'online' state.
        """
        lvol = _make_lvol(status=LVol.STATUS_ONLINE, ha_type="ha")
        mod, _db, _exec, _writes = self._patch(lvol, leader_op_fails=True)

        result = mod.delete_lvol("lvol-uuid-1", force_delete=False)

        # API contract is unchanged (False on internal failure); the
        # important invariant is the persisted state.
        self.assertFalse(result)
        self.assertEqual(lvol.status, LVol.STATUS_IN_DELETION,
                         "lvol must stay in_deletion when leader op fails — "
                         "this is what enables the reconciler path")

    def test_status_in_deletion_when_leader_op_succeeds(self):
        """Sanity: happy path also leaves the lvol in IN_DELETION."""
        lvol = _make_lvol(status=LVol.STATUS_ONLINE, ha_type="ha")
        mod, _db, _exec, _writes = self._patch(lvol, leader_op_fails=False)

        result = mod.delete_lvol("lvol-uuid-1", force_delete=False)

        self.assertTrue(result)
        self.assertEqual(lvol.status, LVol.STATUS_IN_DELETION)

    def test_status_set_before_leader_op_call(self):
        """The IN_DELETION write MUST land on FDB before the leader-side
        delete RPC is invoked. Otherwise a leader RPC that hangs past the
        API timeout would leave the lvol with no persisted intent.
        """
        lvol = _make_lvol(status=LVol.STATUS_ONLINE, ha_type="ha")
        mod, _db, exec_mock, write_calls = self._patch(lvol, leader_op_fails=True)

        # Make execute_on_leader_with_failover assert at-call-time that
        # the lvol it sees in FDB is already in_deletion. We can't read
        # FDB directly here, but we can read the live in-memory lvol —
        # write_to_db has already been called by the time exec_mock runs.
        seen_status = {}

        def _exec_capture(*args, **kwargs):
            seen_status["status_at_call_time"] = lvol.status
            return (False, _make_node(), "fail")

        exec_mock.side_effect = _exec_capture
        mod.delete_lvol("lvol-uuid-1", force_delete=False)

        self.assertEqual(seen_status.get("status_at_call_time"),
                         LVol.STATUS_IN_DELETION,
                         "STATUS_IN_DELETION must be persisted before "
                         "execute_on_leader_with_failover runs")

        # Cross-check via the recorded write_to_db call sequence: the
        # first status snapshot write must be IN_DELETION (the early
        # intent persist), not anything else.
        self.assertTrue(len(write_calls) >= 1,
                        "expected at least one write_to_db call")
        self.assertEqual(write_calls[0], LVol.STATUS_IN_DELETION,
                         "first write_to_db must persist IN_DELETION")

    def test_idempotent_delete_short_circuits(self):
        """A second delete_lvol on an already-IN_DELETION lvol must
        return True without invoking the leader op again — the
        reconciler in lvol_monitor owns the retry loop.
        """
        lvol = _make_lvol(status=LVol.STATUS_IN_DELETION, ha_type="ha")
        mod, _db, exec_mock, _writes = self._patch(lvol, leader_op_fails=False)

        result = mod.delete_lvol("lvol-uuid-1", force_delete=False)

        self.assertTrue(result)
        exec_mock.assert_not_called()  # short-circuit before leader op

    def test_status_set_for_single_ha_type_too(self):
        """Non-HA (single) lvols must also get the early IN_DELETION
        persist. Single is rare in production but the invariant should
        be uniform across both code paths.
        """
        lvol = _make_lvol(status=LVol.STATUS_ONLINE, ha_type="single")
        mod, _db, _exec, _writes = self._patch(lvol, leader_op_fails=False)

        # delete_lvol_from_node is patched to return True in our setup,
        # so the single-path completes successfully.
        result = mod.delete_lvol("lvol-uuid-1", force_delete=False)

        self.assertTrue(result)
        self.assertEqual(lvol.status, LVol.STATUS_IN_DELETION)


# ---------------------------------------------------------------------------
# Source-level invariants
#
# These guard against the IN_DELETION assignment drifting back below the
# ha-type branch in a future refactor. They mirror the source-grep style
# of test_failover_failback_combinations.py so a CI grep failure points
# directly at the regression.
# ---------------------------------------------------------------------------


class TestDeleteLvolSourceOrder(unittest.TestCase):

    def _read_source(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "simplyblock_core", "controllers", "lvol_controller.py",
        )
        with open(path, "r") as f:
            return f.read()

    def _delete_lvol_body(self):
        full = self._read_source()
        start = full.find("def delete_lvol(")
        self.assertGreater(start, 0)
        end = full.find("\ndef ", start + 1)
        return full[start:end] if end > start else full[start:]

    def test_status_assignment_precedes_ha_branch(self):
        body = self._delete_lvol_body()
        in_del_pos = body.find("STATUS_IN_DELETION")
        ha_branch_pos = body.find('lvol.ha_type == "ha"')
        self.assertGreater(in_del_pos, 0,
                            "delete_lvol must reference STATUS_IN_DELETION")
        self.assertGreater(ha_branch_pos, 0,
                            "delete_lvol must branch on ha_type")
        self.assertLess(in_del_pos, ha_branch_pos,
                         "STATUS_IN_DELETION assignment must live BEFORE "
                         "the ha-type branch — otherwise a failed leader "
                         "op leaves the lvol in 'online' and the "
                         "reconciler path in lvol_monitor never fires")

    def test_status_assignment_precedes_leader_failover_call(self):
        body = self._delete_lvol_body()
        # Look for the assignment statement specifically (not the
        # short-circuit check earlier in the function).
        assign_pos = body.find("lvol.status = LVol.STATUS_IN_DELETION")
        exec_call_pos = body.find("execute_on_leader_with_failover(")
        self.assertGreater(assign_pos, 0,
                            "delete_lvol must assign STATUS_IN_DELETION")
        self.assertGreater(exec_call_pos, 0,
                            "delete_lvol must call execute_on_leader_with_failover")
        self.assertLess(assign_pos, exec_call_pos,
                         "STATUS_IN_DELETION must be persisted before "
                         "execute_on_leader_with_failover is invoked")


if __name__ == "__main__":
    unittest.main()
