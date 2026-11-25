"""A secondary register refused because a stale lvol holds the blob id.

Bed bf32 (2026-11-21): a stale lvol stayed registered on two secondaries after
the leader deleted it; the leader reused its blob id, the new lvol shared the
stale lvol's open blob handle, and a delete freed it under the other (SIGSEGV).
SPDK (spdk #98) now refuses that register with -EEXIST; the control plane
removes the stale lvol -- only one it has no live record of -- and retries.
"""
import errno
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import lvol_register
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCRemoteError

LVS = "LVS_7"
BLOB = 0x2A


def _eexist():
    return RPCRemoteError("File exists", code=-errno.EEXIST)


class _Rpc:
    """A secondary with one lvstore; ``lvols``: name -> (uuid, blobid, is_snapshot)."""

    def __init__(self, lvols, refuse=True, delete_error=None, delete_leaves_it=False):
        self.lvols = dict(lvols)
        self.refuse = refuse
        self.delete_error = delete_error
        self.delete_leaves_it = delete_leaves_it
        self.calls: list[tuple] = []
        self.register_fn = None  # replaces bdev_lvol_register when set
        self.delete_fn = None    # replaces delete_lvol when set

    def bdev_lvol_register(self, name, lvs, uuid, blobid, priority_class=0):
        if self.register_fn is not None:
            return self.register_fn(name, lvs, uuid, blobid, priority_class)
        self.calls.append(("register", name, blobid))
        if self.refuse and any(b == blobid for _, b, _s in self.lvols.values()):
            raise _eexist()
        self.lvols[name] = (uuid, blobid, False)
        return f"unique-{name}"

    def bdev_lvol_get_lvols(self, lvs):
        return [{"name": n, "uuid": u, "blobid": b, "is_snapshot": s}
                for n, (u, b, s) in self.lvols.items()]

    def delete_lvol(self, name, sync=False):
        if self.delete_fn is not None:
            return self.delete_fn(name, sync)
        self.calls.append(("delete", name, sync))
        if self.delete_error is not None:
            raise self.delete_error
        if not self.delete_leaves_it:
            self.lvols.pop(name.split("/", 1)[1], None)
        return True

    def bdev_get(self, name):
        short = name.split("/", 1)[1]
        return {"name": name} if short in self.lvols else None


def _db(*records):
    return SimpleNamespace(get_lvols=lambda cluster_id: list(records))


def _rec(name, uuid, status=LVol.STATUS_ONLINE, lvs=LVS):
    return SimpleNamespace(lvs_name=lvs, lvol_bdev=name, lvol_uuid=uuid, status=status,
                           get_id=lambda: f"id-{name}")


def _raise(err):
    def f(*a, **kw):
        raise err
    return f


def _register(rpc, db, name="LVOL_NEW", uuid="u-new"):
    return lvol_register.register_lvol(rpc, name, LVS, uuid, BLOB, 0,
                                       cluster_id="C", node_label="sec1", db=db)


def test_plain_register_is_untouched():
    rpc = _Rpc({}, refuse=True)
    assert _register(rpc, _db()) == "unique-LVOL_NEW"
    assert rpc.calls == [("register", "LVOL_NEW", BLOB)]


def test_bed_bf32_stale_lvol_without_record_is_removed_and_register_retried():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})
    assert _register(rpc, _db()) == "unique-LVOL_NEW"
    assert rpc.calls == [("register", "LVOL_NEW", BLOB),
                         ("delete", f"{LVS}/LVOL_OLD", True),
                         ("register", "LVOL_NEW", BLOB)]
    assert "LVOL_OLD" not in rpc.lvols


def test_stale_lvol_whose_record_is_being_deleted_is_removed():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})
    db = _db(_rec("LVOL_OLD", "u-old", status=LVol.STATUS_IN_DELETION))
    assert _register(rpc, db) == "unique-LVOL_NEW"
    assert ("delete", f"{LVS}/LVOL_OLD", True) in rpc.calls


@pytest.mark.parametrize("status", [LVol.STATUS_ONLINE, LVol.STATUS_IN_CREATION, LVol.STATUS_OFFLINE])
def test_an_lvol_the_control_plane_still_knows_is_never_deleted(status):
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})
    with pytest.raises(RPCRemoteError) as e:
        _register(rpc, _db(_rec("LVOL_OLD", "u-old", status=status)))
    assert e.value.code == -errno.EEXIST
    assert not any(c[0] == "delete" for c in rpc.calls)


def test_known_by_uuid_alone_counts_as_known():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})
    with pytest.raises(RPCRemoteError):
        _register(rpc, _db(_rec("RENAMED", "u-old")))
    assert not any(c[0] == "delete" for c in rpc.calls)


def test_a_record_on_another_lvstore_does_not_protect_it():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})
    assert _register(rpc, _db(_rec("LVOL_OLD", "u-old", lvs="LVS_9"))) == "unique-LVOL_NEW"


def test_a_snapshot_on_the_blob_is_never_deleted():
    rpc = _Rpc({"SNAP_OLD": ("u-snap", BLOB, True)})
    with pytest.raises(RPCRemoteError):
        _register(rpc, _db())
    assert not any(c[0] == "delete" for c in rpc.calls)


def test_a_repeated_register_of_the_same_lvol_succeeds_without_deleting_it():
    rpc = _Rpc({"LVOL_NEW": ("u-new", BLOB, False)})
    assert _register(rpc, _db())
    assert not any(c[0] == "delete" for c in rpc.calls)
    assert "LVOL_NEW" in rpc.lvols


def test_eexist_with_nothing_on_the_blob_raises_the_original_error():
    rpc = _Rpc({})
    rpc.register_fn = _raise(_eexist())
    with pytest.raises(RPCRemoteError) as e:
        _register(rpc, _db())
    assert e.value.code == -errno.EEXIST


def test_a_failed_delete_does_not_register():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)},
               delete_error=RPCRemoteError("busy", code=-errno.EBUSY))
    with pytest.raises(RPCRemoteError) as e:
        _register(rpc, _db())
    assert e.value.code == -errno.EEXIST
    assert [c[0] for c in rpc.calls] == ["register", "delete"]


def test_a_delete_that_leaves_the_lvol_does_not_register():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)}, delete_leaves_it=True)
    with pytest.raises(RPCRemoteError):
        _register(rpc, _db())
    assert [c[0] for c in rpc.calls] == ["register", "delete"]


def test_a_delete_answering_enodev_counts_as_gone():
    rpc = _Rpc({"LVOL_OLD": ("u-old", BLOB, False)})

    def delete(name, sync=False):
        rpc.calls.append(("delete", name, sync))
        rpc.lvols.pop("LVOL_OLD")  # removed meanwhile by something else
        raise RPCRemoteError("No such device", code=-errno.ENODEV)
    rpc.delete_fn = delete
    assert _register(rpc, _db()) == "unique-LVOL_NEW"
    assert [c[0] for c in rpc.calls] == ["register", "delete", "register"]


def test_other_register_errors_pass_through_untouched():
    rpc = _Rpc({})

    def reg(*a, **kw):
        raise RPCRemoteError("No such device", code=-errno.ENODEV)
    rpc.register_fn = reg
    with pytest.raises(RPCRemoteError) as e:
        _register(rpc, _db())
    assert e.value.code == -errno.ENODEV
    assert rpc.calls == []


def test_every_secondary_register_site_goes_through_the_handler():
    import inspect

    from simplyblock_core.controllers import lvol_controller, migration_controller
    from simplyblock_core.services import tasks_runner_lvol_migration
    for mod in (lvol_controller, migration_controller, tasks_runner_lvol_migration):
        src = inspect.getsource(mod)
        assert ".bdev_lvol_register(" not in src, mod.__name__
        assert "register_lvol(" in src, mod.__name__
