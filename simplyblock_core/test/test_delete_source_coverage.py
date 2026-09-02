"""The delete-source coverage gate: never remove a client's source path until
that client demonstrably holds a controller on the target subsystem.

deleteSource on a SHARED subsystem is an nvmf_subsystem_remove_ns over a live
connection — the client kernel drops the path immediately, with none of the
~60s reconnect grace that masks the same race on dedicated subsystems. Run
2026-09-02 ~19:40: the source namespace was removed at t+0, the target path
attached at t+2s, and XFS shut down in between. The gate replaces the blind
time-based delete with an evidence-based one.
"""
import inspect
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_replication_final as runner


CLIENT = "nqn.2014-08.io.simplyblock:uuid:client-1"


def _lvol(uuid, nqn, node_id):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.nqn = nqn
    lvol.node_id = node_id
    lvol.nodes = [node_id]
    return lvol


def _node(controllers_by_nqn):
    node = MagicMock()
    node.status = StorageNode.STATUS_ONLINE
    rpc = MagicMock()
    rpc.nvmf_subsystem_get_controllers.side_effect = \
        lambda nqn: controllers_by_nqn.get(nqn, [])
    node.rpc_client.return_value = rpc
    return node


@pytest.fixture()
def _fast(monkeypatch):
    """Collapse the poll loop so uncovered cases fail fast."""
    monkeypatch.setattr(runner.constants,
                        "REPL_DELETE_SOURCE_COVERAGE_TIMEOUT_SEC", 0)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)


def _wire(monkeypatch, src_node, tgt_node, tgt_lvol):
    db = MagicMock()
    db.get_storage_node_by_id.side_effect = \
        lambda nid: {"SRC_N": src_node, "TGT_N": tgt_node}[nid]
    db.get_lvol_by_id.return_value = tgt_lvol
    monkeypatch.setattr(runner, "db", db)


def _rep(tgt_lvol):
    rep = LVolReplication()
    rep.target_lvol = tgt_lvol
    return rep


def test_covered_client_lets_the_delete_proceed(monkeypatch, _fast):
    src = _lvol("SRC", "nqn.test:lvol:S", "SRC_N")
    tgt = _lvol("TGT", "nqn.test:lvol:S", "TGT_N")
    src_node = _node({"nqn.test:lvol:S": [{"hostnqn": CLIENT}]})
    tgt_node = _node({"nqn.test:lvol:S": [{"hostnqn": CLIENT}]})
    _wire(monkeypatch, src_node, tgt_node, tgt)
    assert runner._await_delete_source_coverage(src, _rep(tgt)) is True


def test_uncovered_client_blocks_the_delete(monkeypatch, _fast):
    """The exact 2026-09-02 shape: the client still rides only the source."""
    src = _lvol("SRC", "nqn.test:lvol:S", "SRC_N")
    tgt = _lvol("TGT", "nqn.test:lvol:S", "TGT_N")
    src_node = _node({"nqn.test:lvol:S": [{"hostnqn": CLIENT}]})
    tgt_node = _node({"nqn.test:lvol:S": []})
    _wire(monkeypatch, src_node, tgt_node, tgt)
    assert runner._await_delete_source_coverage(src, _rep(tgt)) is False


def test_no_connected_client_means_nothing_to_protect(monkeypatch, _fast):
    src = _lvol("SRC", "nqn.test:lvol:S", "SRC_N")
    tgt = _lvol("TGT", "nqn.test:lvol:S", "TGT_N")
    src_node = _node({"nqn.test:lvol:S": []})
    tgt_node = _node({"nqn.test:lvol:S": []})
    _wire(monkeypatch, src_node, tgt_node, tgt)
    assert runner._await_delete_source_coverage(src, _rep(tgt)) is True


def test_coverage_arriving_during_the_poll_unblocks(monkeypatch):
    """A late preconnect attaching mid-wait releases the gate — the normal
    recovery path when the operator's preconnect lands after the flip."""
    monkeypatch.setattr(runner.constants,
                        "REPL_DELETE_SOURCE_COVERAGE_TIMEOUT_SEC", 60)
    monkeypatch.setattr(runner.constants,
                        "REPL_DELETE_SOURCE_COVERAGE_POLL_SEC", 0)
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)

    src = _lvol("SRC", "nqn.test:lvol:S", "SRC_N")
    tgt = _lvol("TGT", "nqn.test:lvol:S", "TGT_N")
    src_node = _node({"nqn.test:lvol:S": [{"hostnqn": CLIENT}]})
    tgt_answers = iter([[], [], [{"hostnqn": CLIENT}]])
    tgt_node = MagicMock()
    tgt_node.status = StorageNode.STATUS_ONLINE
    tgt_rpc = MagicMock()
    tgt_rpc.nvmf_subsystem_get_controllers.side_effect = \
        lambda nqn: next(tgt_answers)
    tgt_node.rpc_client.return_value = tgt_rpc
    _wire(monkeypatch, src_node, tgt_node, tgt)
    assert runner._await_delete_source_coverage(src, _rep(tgt)) is True


def test_without_a_target_reference_the_gate_stands_aside(monkeypatch, _fast):
    """No relationship to verify against: keep the pre-gate delete behavior
    rather than blocking a delete the gate cannot reason about."""
    src = _lvol("SRC", "nqn.test:lvol:S", "SRC_N")
    monkeypatch.setattr(runner, "db", MagicMock())
    assert runner._await_delete_source_coverage(src, None) is True


def test_finalize_delete_is_gated():
    """Guard: the delete-source block must consult the coverage gate and skip
    (not fail the task) when it reports False."""
    src = inspect.getsource(runner._finalize)
    gate = src.index("_await_delete_source_coverage")
    delete = src.index("lvol_controller.delete_lvol")
    assert gate < delete, "coverage must be checked before deleting the source"
    assert "delete-source SKIPPED" in src
