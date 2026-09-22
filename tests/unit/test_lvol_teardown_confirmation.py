"""A teardown is only complete when the node has confirmed it.

R26.3 field report: four lvols existed as bdevs in SPDK with no record in FDB.
The defects behind that which live in this module and are pure logic:

  1. ``_remove_bdev_stack`` returned a constant ``True``. A failed removal was
     logged, the entry was stamped ``status='deleted'`` anyway, and
     ``delete_lvol_from_node`` reported success — so lvol_monitor went on to
     erase the record while the bdev was still registered.

  2. The "is it already gone?" probe was ``if not rpc_client.get_bdevs(name)``.
     ``get_bdevs`` returns ``None`` both for "no such device" and for a non-200
     from the SPDK proxy, so one transient hiccup during a mass delete skipped
     the delete entirely and recorded it as done, leaving nothing in the log
     but an INFO line. The probe now goes through ``RPCClient.bdev_get``
     (tested in ``tests/unit/rpc/test_client.py``), which raises on a genuine
     RPC failure instead of collapsing it into "gone".

  3. ``delete_lvol_from_node`` conflated "removed it", "the node is
     disconnected so I did not try" and "a task owns it": all three were
     ``True``. It now raises ``PreconditionError`` for a deferred teardown and
     ``RuntimeError`` for a failed one, and returns normally only once the
     teardown is confirmed complete.
"""

import pytest

from simplyblock_core.controllers import lvol_controller as lc
from simplyblock_core.rpc_client import RPCException


class _RPC:
    """Minimal SPDK stub. ``probe`` is what ``bdev_get`` answers with: a bdev
    dict, ``None`` (absent), or an exception instance to raise."""

    def __init__(self, probe=({"name": "x"},), delete=(True, None)):
        self._probe = probe
        self._delete = delete
        self.deletes = []

    def bdev_get(self, name):
        if isinstance(self._probe, Exception):
            raise self._probe
        return self._probe

    def delete_lvol(self, name, sync=False, special_delete=False):
        self.deletes.append((name, sync))
        return self._delete


def _stack(**over):
    bdev = {"type": "bdev_lvol", "name": "LVOL_1",
            "params": {"lvs_name": "LVS_1", "name": "LVOL_1"}}
    bdev.update(over)
    return [bdev]


class TestRemoveBdevStack:

    def test_a_failed_delete_is_reported_and_not_marked_deleted(self):
        """The core leak: this used to return True and stamp the entry
        'deleted', so the record was removed over a live bdev."""
        stack = _stack()
        rpc = _RPC(delete=(None, {"code": -16, "message": "device busy"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False
        assert stack[0].get("status") != "deleted", (
            "an unconfirmed bdev must stay unmarked so a retry re-attempts it")

    def test_a_successful_delete_is_confirmed(self):
        stack = _stack()
        rpc = _RPC()
        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.deletes == [("LVS_1/LVOL_1", True)]

    def test_an_absent_bdev_is_confirmed_without_a_delete(self):
        """The optimisation the probe exists for: no second metadata walk."""
        stack = _stack()
        rpc = _RPC(probe=None)

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.deletes == [], "an absent bdev must not be re-deleted"

    def test_a_failed_probe_still_attempts_the_delete(self):
        """A failed probe is not evidence of anything. It used to short-circuit
        to "already deleted, skipping" and drop the delete on the floor."""
        stack = _stack()
        rpc = _RPC(probe=RPCException("proxy returned non-200"))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert rpc.deletes == [("LVS_1/LVOL_1", True)], (
            "a failed probe must fall through to the delete, not skip it")

    def test_a_failed_probe_with_a_failing_delete_is_unconfirmed(self):
        stack = _stack()
        rpc = _RPC(probe=RPCException("proxy returned non-200"),
                   delete=(None, {"code": -16}))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False
        assert stack[0].get("status") != "deleted"

    def test_enodev_from_the_delete_counts_as_confirmation(self):
        stack = _stack()
        rpc = _RPC(probe=RPCException("proxy returned non-200"),
                   delete=(None, {"code": -19, "message": "No such device"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert stack[0]["status"] == "deleted"

    def test_a_clone_entry_follows_the_same_path(self):
        stack = [{"type": "bdev_lvol_clone", "name": "LVS_1/CLN_1"}]
        rpc = _RPC(delete=(None, {"code": -16}))
        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False

    def test_bmap_init_is_not_a_failure(self):
        """It is a bookkeeping entry with no bdev behind it. It fell through to
        the failure log on every delete; now that the result is honest, that
        noise would fail every teardown."""
        stack = [{"type": "bmap_init", "name": "bmap", "params": {}}]
        assert lc._remove_bdev_stack(stack, _RPC(), sync=True) is True
        assert stack[0]["status"] == "deleted"

    def test_one_failure_among_several_fails_the_whole_stack(self):
        stack = [
            {"type": "bdev_lvol", "name": "LVOL_1",
             "params": {"lvs_name": "LVS_1", "name": "LVOL_1"}},
            {"type": "bdev_lvol_clone", "name": "LVS_1/CLN_1"},
        ]

        class _PartialRPC(_RPC):
            def delete_lvol(self, name, sync=False, special_delete=False):
                self.deletes.append((name, sync))
                if name.endswith("CLN_1"):
                    return None, {"code": -16}
                return True, None

        rpc = _PartialRPC()
        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False
        assert stack[0]["status"] == "deleted"
        assert stack[1].get("status") != "deleted"
        assert len(rpc.deletes) == 2, "a failure must not abort the remaining entries"


class TestLvolBdevAbsentOnNode:
    """The post-condition the delete protocol never checked. An acknowledged
    sync-delete RPC is not proof the bdev is gone."""

    class _Node:
        def __init__(self, rpc):
            self._rpc = rpc

        def rpc_client(self, **kw):
            if isinstance(self._rpc, Exception):
                raise self._rpc
            return self._rpc

        def get_id(self):
            return "node-1234abcd"

    class _Lvol:
        lvs_name = "LVS_1"
        lvol_bdev = "LVOL_1"

    def test_absent_when_the_node_says_no_such_device(self):
        node = self._Node(_RPC(probe=None))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is True

    def test_not_absent_when_the_bdev_is_still_listed(self):
        node = self._Node(_RPC(probe={"name": "LVS_1/LVOL_1"}))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is False

    def test_a_failed_probe_raises_instead_of_answering_unknown(self):
        node = self._Node(_RPC(probe=RPCException("proxy returned non-200")))
        with pytest.raises(RPCException):
            lc.lvol_bdev_absent_on_node(self._Lvol(), node)

    def test_no_rpc_client_raises(self):
        node = self._Node(RuntimeError("node is gone"))
        with pytest.raises(RuntimeError):
            lc.lvol_bdev_absent_on_node(self._Lvol(), node)
