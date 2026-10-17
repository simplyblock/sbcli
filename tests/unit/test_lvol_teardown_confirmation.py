"""A teardown is only reported complete once the node has confirmed the bdev
is actually gone.

``_remove_bdev_stack`` marks an entry ``deleted`` only on confirmed removal
(a successful delete, or a probe showing the bdev already absent); a failed
removal is reported as such and left unmarked, so a retry re-attempts it
instead of the record being erased over a bdev still registered in SPDK.

The absence probe is ``RPCClient.bdev_get`` (tested in
``tests/unit/rpc/test_client.py``), which raises on a genuine RPC failure
rather than answering "absent" -- an RPC that could not be completed is not
evidence the bdev is gone, so the code falls through to attempting the
delete instead of skipping it.

``delete_lvol_from_node`` keeps three outcomes distinct rather than
collapsing them into one boolean: teardown completed (returns normally),
deferred (raises ``PreconditionError``), or failed (raises ``RuntimeError``).
"""

import errno

import pytest

from simplyblock_core.controllers import lvol_controller as lc
from simplyblock_core.rpc_client import RPCException, RPCRemoteError


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
        result, error = self._delete
        if error:
            raise RPCRemoteError(error.get("message", ""), error.get("code", 0))
        return result


def _stack(**over):
    bdev = {"type": "bdev_lvol", "name": "LVOL_1",
            "params": {"lvs_name": "LVS_1", "name": "LVOL_1"}}
    bdev.update(over)
    return [bdev]


class TestRemoveBdevStack:

    def test_a_failed_delete_is_reported_and_not_marked_deleted(self):
        """A failed removal must not be marked deleted -- the record would
        then be erased while the bdev is still registered in SPDK."""
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
        """An already-absent bdev needs no delete call -- the probe alone
        confirms teardown."""
        stack = _stack()
        rpc = _RPC(probe=None)

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.deletes == [], "an absent bdev must not be re-deleted"

    def test_a_failed_probe_still_attempts_the_delete(self):
        """A failed probe is not evidence the bdev is gone, so the delete
        must still be attempted rather than skipped."""
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
        """A bmap_init entry is bookkeeping with no bdev behind it, so it
        must not count as a deletion failure."""
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
                    raise RPCRemoteError("device busy", -16)
                return True

        rpc = _PartialRPC()
        assert lc._remove_bdev_stack(stack, rpc, sync=True) is False
        assert stack[0]["status"] == "deleted"
        assert stack[1].get("status") != "deleted"
        assert len(rpc.deletes) == 2, "a failure must not abort the remaining entries"


class _CryptoRPC:
    """Minimal SPDK stub for the crypto branch. ``probe`` is what
    ``bdev_get`` answers with (a bdev dict, ``None``, or an exception
    instance); ``crypto_delete``/``key_delete`` are ``(result, error)``
    pairs, where ``error`` -- a dict with ``code``/``message`` -- makes the
    call raise ``RPCRemoteError`` unless called with ``idempotent=True`` and
    the error is ``-ENODEV``, mirroring the real idempotent-absorption
    contract on ``RPCClient.lvol_crypto_delete``/``lvol_crypto_key_delete``
    (tested directly in ``tests/unit/rpc/test_client.py``)."""

    def __init__(self, probe=({"name": "x"},), crypto_delete=(True, None),
                 key_delete=(True, None)):
        self._probe = probe
        self._crypto_delete = crypto_delete
        self._key_delete = key_delete
        self.crypto_deletes = []
        self.key_deletes = []

    def bdev_get(self, name):
        if isinstance(self._probe, Exception):
            raise self._probe
        return self._probe

    def lvol_crypto_delete(self, name, *, idempotent=False):
        self.crypto_deletes.append(name)
        result, error = self._crypto_delete
        if error:
            if idempotent and error.get("code") == -errno.ENODEV:
                return None
            raise RPCRemoteError(error.get("message", ""), error.get("code", 0))
        return result

    def lvol_crypto_key_delete(self, name, *, idempotent=False):
        self.key_deletes.append(name)
        result, error = self._key_delete
        if error:
            if idempotent and error.get("code") == -errno.ENODEV:
                return None
            raise RPCRemoteError(error.get("message", ""), error.get("code", 0))
        return result


def _crypto_stack(**over):
    bdev = {"type": "crypto", "name": "crypto_LVOL_1"}
    bdev.update(over)
    return [bdev]


class TestRemoveBdevStackCrypto:
    """SFAM-2826: a retried teardown pass reaches the crypto branch after the
    crypto bdev (and/or its key) were already removed on a prior pass.
    ``lvol_crypto_delete``/``lvol_crypto_key_delete`` answer that with
    ``RPCRemoteError(-ENODEV)`` rather than a falsy result -- unhandled, that
    exception used to escape ``_remove_bdev_stack`` entirely, skipping the
    ``all_removed=False`` path, so the delete never converged."""

    def test_a_failed_crypto_delete_is_reported_and_not_marked_deleted(self):
        stack = _crypto_stack()
        rpc = _CryptoRPC(crypto_delete=(None, {"code": -16, "message": "device busy"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=False) is False
        assert stack[0].get("status") != "deleted"

    def test_a_successful_crypto_delete_is_confirmed(self):
        stack = _crypto_stack()
        rpc = _CryptoRPC()

        assert lc._remove_bdev_stack(stack, rpc, sync=False) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.crypto_deletes == ["crypto_LVOL_1"]
        assert rpc.key_deletes == ["key_crypto_LVOL_1"]

    def test_an_absent_crypto_bdev_is_confirmed_without_a_delete(self):
        stack = _crypto_stack()
        rpc = _CryptoRPC(probe=None)

        assert lc._remove_bdev_stack(stack, rpc, sync=False) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.crypto_deletes == [], "an absent bdev must not be re-deleted"

    def test_enodev_from_the_crypto_delete_counts_as_confirmation(self):
        """The RCA scenario: the probe cannot tell (or races the real state),
        and the bdev delete answers -ENODEV. ``_remove_bdev_stack`` must pass
        ``idempotent=True`` for that to be absorbed instead of raising
        uncaught -- the key delete still runs afterward, since the two
        resources have independent absent-signatures (one call does not
        short-circuit the other)."""
        stack = _crypto_stack()
        rpc = _CryptoRPC(probe=RPCException("proxy returned non-200"),
                          crypto_delete=(None, {"code": -19, "message": "No such device"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=False) is True
        assert stack[0]["status"] == "deleted"
        assert rpc.key_deletes == ["key_crypto_LVOL_1"], "the key delete still runs independently"

    def test_enodev_from_the_key_delete_counts_as_confirmation(self):
        """The bdev delete succeeds but the key was already removed on a
        prior pass -- the same idempotent absorption applies one call later."""
        stack = _crypto_stack()
        rpc = _CryptoRPC(key_delete=(None, {"code": -19, "message": "No such device"}))

        assert lc._remove_bdev_stack(stack, rpc, sync=False) is True
        assert stack[0]["status"] == "deleted"

    def test_sync_deletes_skip_the_crypto_branch_entirely(self):
        """A sync (non-leader) teardown never touches crypto state -- the
        branch's own ``and not sync`` guard must still hold."""
        stack = _crypto_stack()
        rpc = _CryptoRPC()

        assert lc._remove_bdev_stack(stack, rpc, sync=True) is True
        assert rpc.crypto_deletes == []
        assert stack[0].get("status") != "deleted"


class TestLvolBdevAbsentOnNode:
    """The post-condition the delete protocol depends on: an acknowledged
    sync-delete RPC is not proof the bdev is gone, so callers must verify it
    separately before treating a teardown as complete."""

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
