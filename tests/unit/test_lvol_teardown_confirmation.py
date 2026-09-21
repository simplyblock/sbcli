"""A teardown is only complete when the node has confirmed it.

R26.3 field report: four lvols existed as bdevs in SPDK with no record in FDB.
Three of the defects behind that live in this module and are pure logic:

  1. ``_remove_bdev_stack`` returned a constant ``True``. A failed removal was
     logged, the entry was stamped ``status='deleted'`` anyway, and
     ``delete_lvol_from_node`` reported success — so lvol_monitor went on to
     erase the record while the bdev was still registered.

  2. The "is it already gone?" probe was ``if not rpc_client.get_bdevs(name)``.
     ``get_bdevs`` returns ``None`` both for "no such device" and for a non-200
     from the SPDK proxy, so one transient hiccup during a mass delete skipped
     the delete entirely and recorded it as done, leaving nothing in the log
     but an INFO line.

  3. ``delete_lvol_from_node`` conflated "removed it", "the node is
     disconnected so I did not try" and "a task owns it": all three were
     ``True``. It now returns ``True`` only when the teardown is confirmed
     complete on the node.
"""

from simplyblock_core.controllers import lvol_controller as lc


class _RPC:
    """Minimal SPDK stub. ``probe`` is what get_bdevs_2 answers with, as the
    ``(result, error)`` pair ``_request2`` really returns."""

    def __init__(self, probe=([{"name": "x"}], None), delete=(True, None)):
        self._probe = probe
        self._delete = delete
        self.deletes = []

    def get_bdevs_2(self, name):
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


class TestBdevPresenceProbe:
    """``_bdev_present`` must never answer "absent" for a question the node
    did not actually answer."""

    def test_present_when_the_node_lists_it(self):
        assert lc._bdev_present(_RPC(probe=([{"name": "x"}], None)), "LVS_1/LVOL_1") is True

    def test_absent_on_enodev(self):
        rpc = _RPC(probe=(None, {"code": -19, "message": "No such device"}))
        assert lc._bdev_present(rpc, "LVS_1/LVOL_1") is False

    def test_absent_on_an_empty_list(self):
        assert lc._bdev_present(_RPC(probe=([], None)), "LVS_1/LVOL_1") is False

    def test_unknown_when_the_proxy_returns_non_200(self):
        # _request2 yields (None, None) for a non-200 — indistinguishable from
        # "gone" through get_bdevs, which is the whole bug.
        assert lc._bdev_present(_RPC(probe=(None, None)), "LVS_1/LVOL_1") is None

    def test_unknown_on_an_unrelated_rpc_error(self):
        rpc = _RPC(probe=(None, {"code": -110, "message": "timeout"}))
        assert lc._bdev_present(rpc, "LVS_1/LVOL_1") is None

    def test_unknown_when_the_call_raises(self):
        assert lc._bdev_present(_RPC(probe=RuntimeError("connection error")),
                                "LVS_1/LVOL_1") is None


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
        rpc = _RPC(probe=(None, {"code": -19, "message": "No such device"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.deletes == [], "an absent bdev must not be re-deleted"

    def test_an_unknown_probe_still_attempts_the_delete(self):
        """A failed probe is not evidence of anything. It used to short-circuit
        to "already deleted, skipping" and drop the delete on the floor."""
        stack = _stack()
        rpc = _RPC(probe=(None, None))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert rpc.deletes == [("LVS_1/LVOL_1", True)], (
            "an unknown probe must fall through to the delete, not skip it")

    def test_an_unknown_probe_with_a_failing_delete_is_unconfirmed(self):
        stack = _stack()
        rpc = _RPC(probe=(None, None), delete=(None, {"code": -16}))

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False
        assert stack[0].get("status") != "deleted"

    def test_enodev_from_the_delete_counts_as_confirmation(self):
        stack = _stack()
        rpc = _RPC(probe=(None, None),
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
        node = self._Node(_RPC(probe=(None, {"code": -19})))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is True

    def test_not_absent_when_the_bdev_is_still_listed(self):
        node = self._Node(_RPC(probe=([{"name": "LVS_1/LVOL_1"}], None)))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is False

    def test_unknown_is_not_absent(self):
        node = self._Node(_RPC(probe=(None, None)))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is None

    def test_unknown_when_no_rpc_client_can_be_built(self):
        node = self._Node(RuntimeError("node is gone"))
        assert lc.lvol_bdev_absent_on_node(self._Lvol(), node) is None
