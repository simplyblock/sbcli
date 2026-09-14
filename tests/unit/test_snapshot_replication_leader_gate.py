"""The LVS-leader gate in the snapshot-replication service.

Pure logic over a mocked ``is_node_leader`` — no model state, no DB — so it
stays in the unit tier. The retention tests it used to share a file with are
DB-driven and live in ``tests/integration/test_snapshot_replication_retention.py``.
"""

from simplyblock_core.services import snapshot_replication as sr


def test_require_lvs_leader_gate(monkeypatch):
    """Convert on a non-leader returns success WITHOUT persisting (silent
    conversion error) — leadership must be checked BEFORE the operation and a
    non-leader must fail-and-retry, never proceed."""
    import simplyblock_core.controllers.lvol_controller as lc

    class _N:
        def get_id(self):
            return "N1"

    monkeypatch.setattr(lc, "is_node_leader", lambda node, lvs: False)
    assert sr._require_lvs_leader(_N(), "LVS_1", "convert") is False

    monkeypatch.setattr(lc, "is_node_leader", lambda node, lvs: True)
    assert sr._require_lvs_leader(_N(), "LVS_1", "convert") is True
