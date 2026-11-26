"""bf36 R3a (2026-11-26): a target lvstore has no client IO, so nothing promoted
its secondary when the primary went down; the transfer sat suspended until the
restarted primary took the lvstore back (~2 min). With the primary down, the
transfer now goes to the secondary's hub: that IO promotes it, as client IO
promotes a secondary (SPDK decides; the control plane grants nothing).
"""
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr

ONLINE = StorageNode.STATUS_ONLINE


def _node(nid, status=ONLINE, secondary="", lvstore=None, rpc_error=None):
    rpc = MagicMock()
    if rpc_error:
        rpc.get_lvstore.side_effect = rpc_error
    else:
        rpc.get_lvstore.return_value = lvstore
    return SimpleNamespace(get_id=lambda: nid, status=status,
                           secondary_node_id=secondary, rpc_client=lambda **kw: rpc)


def _run(nodes):
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
    remote_lv = SimpleNamespace(node_id="P", lvs_name="LVS_25")
    with patch.object(sr, "db", db):
        return sr._io_promotion_receiver(remote_lv)


def test_the_secondary_receives_while_the_primary_restarts():
    sec, why = _run({"P": _node("P", StorageNode.STATUS_RESTARTING, secondary="S"),
                     "S": _node("S", lvstore={"name": "LVS_25"})})
    assert sec.get_id() == "S"
    assert "promotes it" in why


def test_the_secondary_receives_while_the_primary_is_offline():
    sec, _ = _run({"P": _node("P", StorageNode.STATUS_OFFLINE, secondary="S"),
                   "S": _node("S", lvstore={"name": "LVS_25"})})
    assert sec.get_id() == "S"


def test_an_online_primary_is_left_to_the_guarded_recovery():
    sec, why = _run({"P": _node("P", ONLINE, secondary="S"),
                     "S": _node("S", lvstore={"name": "LVS_25"})})
    assert sec is None and "online" in why


def test_not_to_a_secondary_that_is_not_online():
    sec, why = _run({"P": _node("P", StorageNode.STATUS_OFFLINE, secondary="S"),
                     "S": _node("S", StorageNode.STATUS_OFFLINE)})
    assert sec is None and "secondary" in why


def test_not_to_a_secondary_that_does_not_answer():
    sec, why = _run({"P": _node("P", StorageNode.STATUS_OFFLINE, secondary="S"),
                     "S": _node("S", rpc_error=RuntimeError("Could not reach remote"))})
    assert sec is None and "does not answer" in why


def test_not_to_a_secondary_without_the_lvstore():
    sec, why = _run({"P": _node("P", StorageNode.STATUS_OFFLINE, secondary="S"),
                     "S": _node("S", lvstore=None)})
    assert sec is None and "does not hold" in why


def test_not_without_a_secondary():
    sec, _ = _run({"P": _node("P", StorageNode.STATUS_OFFLINE, secondary="")})
    assert sec is None


def test_the_start_sends_to_the_secondary_instead_of_waiting():
    src = inspect.getsource(sr.process_snap_replicate_start)
    gate = src.index("_stable_target_leader(remote_lv)")
    promo = src.index("_io_promotion_receiver(remote_lv)", gate)
    wait = src.index("_wait_for_target_leader(", gate)
    assert promo < wait, "the secondary is tried before the transfer waits"
    assert "not io_promotion and not _require_lvs_leader(" in src
