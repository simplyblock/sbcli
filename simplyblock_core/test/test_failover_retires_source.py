"""Fail-over must retire the still-alive source's data path.

The source is only assumed dead. Left published, the client keeps writing to
the superseded original (discarded at fail-back), and a namespaced volume's
restage re-attaches to the stale ORIGINAL device — same model/nsid glob, DR
clone rejected against the original-identity head — so pods never reach the
DR cluster (run 2026-09-03 ~09:00: all three namespaced pods remounted on the
originals after fail-over; fail-back's eviction then shut down all three
filesystems mid-IO).
"""
import inspect
from unittest.mock import MagicMock

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.controllers import lvol_controller as lc


def _lvol():
    lvol = LVol()
    lvol.uuid = "SRC"
    lvol.nqn = "nqn.test:lvol:S"
    lvol.node_id = "N1"
    lvol.nodes = ["N1", "N2"]
    return lvol


def _node(status=StorageNode.STATUS_ONLINE):
    node = MagicMock()
    node.status = status
    return node


def _record(bucket, value):
    """Append and report success — a lambda-safe stand-in for the patched
    call sites, which must return truthy."""
    bucket.append(value)
    return True


def test_retire_fences_then_removes_on_every_online_node(monkeypatch):
    fenced: list = []
    removed: list = []
    monkeypatch.setattr(lc, "suspend_lvol",
                        lambda lvol_id: _record(fenced, lvol_id))
    monkeypatch.setattr(lc, "_remove_lvol_subsys_from_node",
                        lambda lvol, rpc: _record(removed, lvol.get_id()))
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = \
        lambda nid: {"N1": _node(), "N2": _node()}[nid]
    lc._retire_source_data_path(db, _lvol())
    assert fenced == ["SRC"]
    assert removed == ["SRC", "SRC"], "namespace removed on primary AND peer"


def test_retire_skips_offline_nodes(monkeypatch):
    removed: list = []
    monkeypatch.setattr(lc, "suspend_lvol", lambda lvol_id: True)
    monkeypatch.setattr(lc, "_remove_lvol_subsys_from_node",
                        lambda lvol, rpc: _record(removed, rpc))
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = lambda nid: {
        "N1": _node(), "N2": _node(status=StorageNode.STATUS_OFFLINE)}[nid]
    lc._retire_source_data_path(db, _lvol())
    assert len(removed) == 1


def test_retire_tolerates_an_unreachable_source(monkeypatch):
    """A genuine DR event: fencing raises, nodes unknown — the fail-over must
    not be aborted by its own best-effort cleanup."""
    def _boom(lvol_id):
        raise RuntimeError("source cluster unreachable")

    monkeypatch.setattr(lc, "suspend_lvol", _boom)
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = KeyError("gone")
    lc._retire_source_data_path(db, _lvol())  # must not raise


def test_failover_retires_the_source_after_the_relationship_is_durable():
    """Ordering guard: by the time the source path disappears, connect_lvol
    must already resolve the volume to the DR copy — so the relationship
    write comes first, the retire after."""
    src = inspect.getsource(lc.replicate_lvol_on_target_cluster)
    rel = src.index("lvol_replication.write_to_db")
    retire = src.index("_retire_source_data_path")
    assert rel < retire


def test_monitor_skips_retired_sources():
    """The monitor's health check would fail on the deliberately-removed
    namespace and its self-heal would register it back — resurrecting the
    stale device the retirement removed."""
    from simplyblock_core.services import lvol_monitor
    src = inspect.getsource(lvol_monitor)
    guard = src.index('from_source')
    check = src.index("check_subsystem")
    assert guard < check, "the from_source guard must run before any check"


def test_repair_refuses_retired_sources(monkeypatch):
    from simplyblock_core import storage_node_ops as ops

    lvol = _lvol()
    lvol.status = LVol.STATUS_ONLINE
    lvol.from_source = False
    lvol.lvs_name = "LVS_1"

    monkeypatch.setattr(ops, "get_restart_phase", lambda *a: "")
    db = MagicMock()
    db.get_lvol_by_id.return_value = lvol
    monkeypatch.setattr(ops, "DBController", lambda: db)

    ok, err = ops.repair_lvol_registration_on_non_leader(lvol, _node(), 0)
    assert ok is False
    assert "retired source" in err
