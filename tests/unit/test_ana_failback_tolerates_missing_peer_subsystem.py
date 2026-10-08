"""The ANA failback a restarting primary runs against its secondary must not
abort on a peer that cannot flip a listener.

Incident 2026-10-08 (run 62): after an SPDK abort cascade every SPDK pod was
recreated by Kubernetes, so the secondaries held no subsystem for their
primaries' volumes. cluster_activate's lvstore recreate called
_failback_primary_ana unguarded, SPDK answered the ANA flip with "Unable to
find subsystem", the RPC client raised, the recreate thread died and the
activation ended in "Failed to activate cluster" -- 100+ times in a row, since
the monitor retried it every tick. The restart path has always caught that
error (trigger_ana_failback_for_node); the recreate path now does too, and the
flip itself keeps going past the volumes the peer cannot serve.
"""

from unittest.mock import MagicMock, call, patch

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.rpc_client import RPCException


def _lvol(nqn, ns_id=1, lvs="LVS_1"):
    lv = MagicMock()
    lv.nqn, lv.ns_id, lv.lvs_name = nqn, ns_id, lvs
    return lv


def _peer():
    peer = MagicMock()
    peer.get_id.return_value = "peer-1"
    return peer


class TestDemoteListenersOnPeer:

    def test_a_missing_subsystem_is_skipped_and_the_rest_still_demoted(self):
        lvols = [_lvol("nqn:a"), _lvol("nqn:b"), _lvol("nqn:c")]
        peer = _peer()

        def flip(lvol, node, state):
            if lvol.nqn == "nqn:b":
                raise RPCException("Unable to find subsystem with NQN nqn:b")

        with patch.object(ops, "_set_lvol_ana_on_node", side_effect=flip) as set_ana:
            failed = ops._demote_listeners_on_peer(lvols, peer)

        assert failed == ["nqn:b"]
        assert set_ana.call_args_list == [
            call(lvols[0], peer, "non_optimized"),
            call(lvols[1], peer, "non_optimized"),
            call(lvols[2], peer, "non_optimized"),
        ]

    def test_every_listener_demoted_returns_an_empty_list(self):
        lvols = [_lvol("nqn:a"), _lvol("nqn:b")]
        with patch.object(ops, "_set_lvol_ana_on_node") as set_ana:
            assert ops._demote_listeners_on_peer(lvols, _peer()) == []
        assert set_ana.call_count == 2

    def test_namespaces_of_one_subsystem_are_flipped_once_each(self):
        # A namespaced subsystem carries several volumes: one ANA group per
        # namespace, and a duplicate record for the same namespace costs
        # nothing.
        lvols = [_lvol("nqn:ns", ns_id=1), _lvol("nqn:ns", ns_id=2), _lvol("nqn:ns", ns_id=2)]
        with patch.object(ops, "_set_lvol_ana_on_node") as set_ana:
            ops._demote_listeners_on_peer(lvols, _peer())
        assert [(c.args[0].ns_id) for c in set_ana.call_args_list] == [1, 2]

    def test_an_unexpected_error_still_propagates(self):
        # Only the RPC refusal is a per-volume condition; anything else is a
        # bug the caller must see.
        with patch.object(ops, "_set_lvol_ana_on_node", side_effect=TypeError("boom")):
            try:
                ops._demote_listeners_on_peer([_lvol("nqn:a")], _peer())
            except TypeError:
                pass
            else:
                raise AssertionError("TypeError was swallowed")
