"""The wire namespace identity (LVol.get_ns_uuid) is what every verify,
register, and remove site must use — never the record uuid.

After a fail-back the two differ by design: the record carries the restored
original uuid while the namespace keeps advertising the DR-generation wire
identity the client's multipath head holds. Run 2026-09-02 ~19:40: the health
check compared the record uuid, flagged every failed-back volume unhealthy
(Health: False), and the lvol monitor's self-heal then re-registered the
namespace under the record uuid — an identity the client kernel rejects
("IDs don't match for shared namespace N") — severing live paths.
"""
import inspect
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol


def _failed_back_lvol():
    lvol = LVol()
    lvol.uuid = "REC"
    lvol.ns_uuid = "WIRE"
    lvol.nqn = "nqn.test:lvol:SHARED"
    lvol.ns_id = 1
    lvol.top_bdev = "LVS_1/LVOL_C"
    lvol.status = LVol.STATUS_ONLINE
    return lvol


def test_get_ns_uuid_prefers_the_borrowed_wire_identity():
    lvol = _failed_back_lvol()
    assert lvol.get_ns_uuid() == "WIRE"
    lvol.ns_uuid = ""
    assert lvol.get_ns_uuid() == "REC"


def test_health_check_verifies_the_wire_identity(monkeypatch):
    """check_lvol_on_node must hand check_subsystem the WIRE identity, or a
    healthy failed-back volume reports unhealthy forever."""
    from simplyblock_core.controllers import health_controller as hc

    lvol = _failed_back_lvol()
    lvol.bdev_stack = []

    db = MagicMock()
    db.get_lvol_by_id.return_value = lvol
    db.get_storage_node_by_id.return_value = MagicMock()

    captured = {}

    def _fake_check_subsystem(nqn, *, rpc_client=None, nqns=None, ns_uuid=None):
        captured["ns_uuid"] = ns_uuid
        return True

    with patch.object(hc, "DBController", lambda: db), \
            patch.object(hc, "check_subsystem", _fake_check_subsystem):
        assert hc.check_lvol_on_node("REC", "N1") is True
    assert captured["ns_uuid"] == "WIRE"


def test_delete_removes_the_namespace_by_its_wire_identity(monkeypatch):
    """_remove_lvol_subsys_from_node matched ns['uuid'] against the record
    uuid; for a failed-back volume that never matches, and the namespace is
    left behind on the shared subsystem forever."""
    from simplyblock_core.controllers import lvol_controller as lc

    lvol = _failed_back_lvol()

    removed = []

    class _Rpc:
        def __init__(self):
            self.namespaces = [
                {"nsid": 1, "uuid": "WIRE", "bdev_name": "LVS_1/LVOL_C"},
                {"nsid": 2, "uuid": "SIBLING", "bdev_name": "LVS_1/LVOL_S"},
            ]

        def subsystem_get(self, nqn):
            return {"nqn": nqn, "namespaces": list(self.namespaces)}

        def nvmf_subsystem_remove_ns(self, nqn, nsid):
            removed.append(nsid)
            self.namespaces = [n for n in self.namespaces if n["nsid"] != nsid]
            return True

    monkeypatch.setattr(lc.time, "sleep", lambda s: None)
    assert lc._remove_lvol_subsys_from_node(lvol, _Rpc()) is True
    assert removed == [1]


def test_registration_sites_use_the_wire_identity():
    """Every path that (re)registers an lvol namespace must pass
    get_ns_uuid(): the monitor's self-heal, node-restart recreation, and
    intra-cluster migration all re-run against records whose namespace may
    carry a borrowed identity."""
    from simplyblock_core import storage_node_ops
    from simplyblock_core.controllers import lvol_controller, migration_controller

    for func in (storage_node_ops.add_lvol_thread,
                 lvol_controller.recreate_lvol_on_node,
                 migration_controller.create_migration):
        src = inspect.getsource(func)
        assert "get_ns_uuid()" in src, \
            f"{func.__name__} must register/verify by the wire identity"
        assert "lvol.uuid, lvol.guid" not in src, \
            f"{func.__name__} still registers by the record uuid"
