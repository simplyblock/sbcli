"""Which namespace the fail-back eviction removes.

The cutover clone needs its preserved nsid; the still-live superseded original
occupies it. On a SHARED (namespaced) subsystem the original's namespace
carries the ORIGINAL record's uuid, so the own-uuid match cannot see it and the
nsid fallback is forbidden (it could evict a live sibling). The eviction must
therefore match the superseded original's identity explicitly — and never touch
siblings (2026-09-02: shared subsystem holding nsids 1-5, clone wanted nsid 1,
add_ns -32602 on every retry).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller


class _FakeRpc:
    """Subsystem with mutable namespaces; remove_ns takes effect immediately."""

    def __init__(self, namespaces):
        self.namespaces = list(namespaces)
        self.removed = []

    def subsystem_get(self, nqn):
        return {"nqn": nqn, "namespaces": list(self.namespaces)}

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        self.removed.append(nsid)
        self.namespaces = [ns for ns in self.namespaces if ns["nsid"] != nsid]
        return True


def _node(rpc):
    node = MagicMock()
    node.secondary_node_id = ""
    node.tertiary_node_id = ""
    node.rpc_client.return_value = rpc
    node.get_id.return_value = "N_tgt"
    return node


def _clone(nsid=1):
    return SimpleNamespace(uuid="CLONE_UUID", ns_id=nsid,
                           top_bdev="LVS/CLONE_BDEV", nqn="nqn.shared")


_ORIGINAL = SimpleNamespace(uuid="ORIG_UUID", lvol_uuid="ORIG_BDEV",
                            top_bdev="LVS/ORIG")

_SHARED = [
    {"nsid": 1, "uuid": "ORIG_UUID", "bdev_name": "ORIG_BDEV"},
    {"nsid": 2, "uuid": "SIB_A", "bdev_name": "SIB_A_BDEV"},
    {"nsid": 3, "uuid": "SIB_B", "bdev_name": "SIB_B_BDEV"},
]


def _evict(rpc, superseded):
    with patch.object(lvol_controller, "DBController", MagicMock()):
        lvol_controller._evict_stale_namespace(_clone(), _node(rpc),
                                               superseded=superseded)


def test_superseded_original_is_evicted_on_a_shared_subsystem():
    rpc = _FakeRpc(_SHARED)
    _evict(rpc, _ORIGINAL)
    assert rpc.removed == [1]


def test_siblings_survive_even_when_the_clone_wants_their_nsid():
    """nsid never identifies a namespace on a shared subsystem."""
    rpc = _FakeRpc([ns for ns in _SHARED if ns["nsid"] != 1])
    _evict(rpc, None)
    assert rpc.removed == []


def test_without_superseded_the_shared_original_stays():
    """The pre-fix behavior: own-uuid match alone cannot see the original.
    Guards that passing superseded is what makes the eviction possible."""
    rpc = _FakeRpc(_SHARED)
    _evict(rpc, None)
    assert rpc.removed == []


def test_matches_by_bdev_name_when_ns_uuid_diverged():
    """SPDK reports lvol namespaces' bdev_name as the raw lvol_uuid; a
    namespace registered with a different ns uuid is still the original's."""
    rpc = _FakeRpc([{"nsid": 1, "uuid": "SOMETHING_ELSE",
                     "bdev_name": "ORIG_BDEV"}] + _SHARED[1:])
    _evict(rpc, _ORIGINAL)
    assert rpc.removed == [1]


def test_dedicated_subsystem_still_evicts_by_nsid():
    """Single-namespace subsystems keep the nsid fallback: there is nobody
    else the match could hit."""
    rpc = _FakeRpc([{"nsid": 1, "uuid": "OLD", "bdev_name": "OLD_BDEV"}])
    _evict(rpc, None)
    assert rpc.removed == [1]
