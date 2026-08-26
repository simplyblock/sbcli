"""The transfer hublvol must be verified on the TARGET, not trusted from the DB.

A restart wipes SPDK's bdevs and subsystems while ``StorageNode.transfer_hublvol``
survives in the DB. ensure_hub_attached used to skip creation whenever that record
was present, so the source attached to a subsystem that no longer existed:
bdev_nvme_attach_controller returned -5 (EIO) and afterwards "Controller ... does
not exist". That was every fail-back into a restarted node in the 2026-08-17/18
labs — 215 failures, exclusively against the two nodes that had been killed and
brought back.

The restart flow does recreate it (storage_node_ops: create_transfer_hublvol after
recreate_hublvol), but only in the non-takeover branch, only when
``not activation_mode`` and only when the LVS has secondaries — and if that call
raises, its handler clears the in-memory object while ``defer_db_write=True``
leaves the stale record in the DB. Healing at use time covers all of those.
"""
from simplyblock_core.services import replication_final_step as rfs


class _Hub:
    bdev_name = "LVS_1/transferhub"
    nqn = "nqn.2023-02.io.simplyblock:CL:transferhub:LVS_1"
    nvmf_port = 4427

    def get_remote_bdev_name(self):
        return "LVS_1/transferhubn1"


_DEFAULT_HUB = _Hub()


class _Nic:
    ip4_address = "10.0.0.1"
    trtype = "TCP"


class _TargetRPC:
    """SPDK on the target: `bdev` and `subsystem` model what survived a restart."""

    def __init__(self, bdev=True, subsystem=True, raises=False):
        self._bdev = bdev
        self._subsystem = subsystem
        self._raises = raises

    def get_bdevs(self, name=None):
        if self._raises:
            raise RuntimeError("target unreachable")
        return [{"name": name}] if self._bdev else []

    def subsystem_get(self, nqn):
        if self._raises:
            raise RuntimeError("target unreachable")
        return {"nqn": nqn} if self._subsystem else None


class _SourceRPC:
    """SPDK on the source: records the attach attempts."""

    def __init__(self, remote_present=False, attach_ok=True):
        self._remote_present = remote_present
        self._attach_ok = attach_ok
        self.attaches = []

    def get_bdevs(self, name=None):
        return [{"name": name}] if self._remote_present else []

    def bdev_nvme_attach_controller(self, *args, **kwargs):
        self.attaches.append(args)
        return ["ok"] if self._attach_ok else None

    def bdev_nvme_controller_list(self, *args, **kwargs):
        return []

    def bdev_nvme_detach_controller(self, *args, **kwargs):
        return ["ok"]


class _TargetNode:
    def __init__(self, rpc, hub=_DEFAULT_HUB):
        self._rpc = rpc
        self.transfer_hublvol = hub
        self.active_rdma = False
        self.data_nics = [_Nic()]
        self.created = 0

    def get_id(self):
        return "N_TGT"

    def rpc_client(self):
        return self._rpc

    def create_transfer_hublvol(self, defer_db_write=False):
        self.created += 1
        # Mirror the real method: it heals SPDK state.
        self._rpc._bdev = True
        self._rpc._subsystem = True
        self.transfer_hublvol = _Hub()


def test_live_hub_is_not_recreated():
    tgt = _TargetNode(_TargetRPC(bdev=True, subsystem=True))
    src = _SourceRPC()
    bdev, remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err is None and tgt.created == 0
    assert bdev == _Hub.bdev_name and remote == _Hub().get_remote_bdev_name()
    assert src.attaches, "a live hub must still be attached on the source"


def test_missing_bdev_after_restart_is_healed():
    """The regression: DB record present, SPDK bdev gone."""
    tgt = _TargetNode(_TargetRPC(bdev=False, subsystem=True))
    src = _SourceRPC()
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err is None
    assert tgt.created == 1, "a stale DB record must not stop the hub being recreated"
    assert src.attaches, "attach must happen after healing"


def test_missing_subsystem_after_restart_is_healed():
    """The bdev can survive while the subsystem the source connects to does not."""
    tgt = _TargetNode(_TargetRPC(bdev=True, subsystem=False))
    src = _SourceRPC()
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err is None and tgt.created == 1


def test_absent_db_record_still_creates():
    tgt = _TargetNode(_TargetRPC(bdev=False, subsystem=False), hub=None)
    src = _SourceRPC()
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err is None and tgt.created == 1


def test_failed_recreation_reports_an_error_instead_of_attaching():
    """Never hand the caller a gateway that does not exist -- that is how the
    -5 EIO became 215 silent retries."""
    class _StubbornNode(_TargetNode):
        def create_transfer_hublvol(self, defer_db_write=False):
            self.created += 1          # claims success, heals nothing

    tgt = _StubbornNode(_TargetRPC(bdev=False, subsystem=False))
    src = _SourceRPC()
    bdev, remote, err = rfs.ensure_hub_attached(src, tgt)
    assert bdev is None and remote is None
    assert err and "still absent" in err
    assert not src.attaches, "must not attach to a hub that was never created"


def test_creation_exception_is_reported():
    class _RaisingNode(_TargetNode):
        def create_transfer_hublvol(self, defer_db_write=False):
            raise RuntimeError("hublvol create failed")

    tgt = _RaisingNode(_TargetRPC(bdev=False, subsystem=False))
    src = _SourceRPC()
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err and "Failed to (re)create the transfer hublvol" in err


def test_unreachable_target_is_not_recreated_blindly():
    """If the target cannot be queried, leave it alone: the attach fails and the
    task retries, which is safer than creating against a node we cannot see."""
    tgt = _TargetNode(_TargetRPC(raises=True))
    src = _SourceRPC(attach_ok=False)
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert tgt.created == 0
    assert err and "Failed to attach transfer hub controller" in err


def test_already_attached_source_short_circuits():
    """A prior iteration or crash recovery left the remote bdev in place."""
    tgt = _TargetNode(_TargetRPC(bdev=True, subsystem=True))
    src = _SourceRPC(remote_present=True)
    _bdev, _remote, err = rfs.ensure_hub_attached(src, tgt)
    assert err is None and not src.attaches
