"""The promote that finishes a node's relocation either settles every device
or says so.

Talos lab 2026-11-12, StorageNodeOps Migrate of a node to another host: the
three origin devices, marked removed by the restart, had been set back online
by the self-repair. device_set_failed refused each ("Device must be in
removed status, current status: online"), the promote logged it and returned
True, the operator reported the migration Succeeded, and the old devices
stayed in the cluster map as live over a volume that was by then corrupt.
"""
import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.exceptions import PromoteIncompleteError
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

NODE_ID = "703754f2-e133-4939-9df4-3e33eea68038"


def _device(uuid, status):
    d = NVMeDevice()
    d.uuid = uuid
    d.status = status
    return d


class _Lvol:
    def __init__(self):
        self.hostname = "worker-2"
        self.written = False

    def write_to_db(self, *a, **kw):
        self.written = True


class _FakeDB:
    """Hands out the node as the promote sees it before its calls, and the
    re-read after them; the second read is what the test scripts."""

    def __init__(self, before, after, lvols=()):
        self.reads = [before, after]
        self.lvols = list(lvols)
        self.kv_store = object()
        self.written = []

    def get_storage_node_by_id(self, node_id):
        node = self.reads.pop(0) if len(self.reads) > 1 else self.reads[0]
        node.write_to_db = lambda *a, **kw: self.written.append(node)
        return node

    def get_lvols_by_node_id(self, node_id):
        return self.lvols


def _node(*devices):
    snode = StorageNode()
    snode.uuid = NODE_ID
    snode.hostname = "worker-4"
    snode.mgmt_ip = "10.244.5.188"
    snode.primary_ip = "10.244.1.214"
    snode.nvme_devices = list(devices)
    return snode


@pytest.fixture
def promote_env(monkeypatch):
    """Install the fakes and record what the promote asked of the device
    controller. ``settle`` says whether the calls take effect in the
    re-read."""
    calls = []

    def install(before, after, add_ok=True, fail_ok=True, lvols=()):
        db = _FakeDB(before, after, lvols)
        monkeypatch.setattr(storage_node_ops, "DBController", lambda: db)
        monkeypatch.setattr(storage_node_ops.time, "sleep", lambda s: None)
        monkeypatch.setattr(storage_node_ops.device_controller, "add_device",
                            lambda device_id, add_migration_task=True: calls.append(("add", device_id)) or add_ok)
        monkeypatch.setattr(storage_node_ops.device_controller, "device_set_failed",
                            lambda device_id: calls.append(("fail", device_id)) or fail_ok)
        return db, calls

    return install


class TestASettledNodeIsPromoted:
    def test_new_devices_are_activated_and_removed_ones_retired(self, promote_env):
        before = _node(_device("new-1", NVMeDevice.STATUS_NEW),
                       _device("old-1", NVMeDevice.STATUS_REMOVED),
                       _device("keep", NVMeDevice.STATUS_ONLINE))
        after = _node(_device("new-1", NVMeDevice.STATUS_ONLINE),
                      _device("old-1", NVMeDevice.STATUS_FAILED),
                      _device("keep", NVMeDevice.STATUS_ONLINE))
        db, calls = promote_env(before, after)

        assert storage_node_ops.make_sec_new_primary(NODE_ID) is True
        assert ("add", "new-1") in calls and ("fail", "old-1") in calls
        assert ("add", "keep") not in calls and ("fail", "keep") not in calls

    def test_the_node_and_its_volumes_are_re_pointed(self, promote_env):
        lvol = _Lvol()
        after = _node(_device("keep", NVMeDevice.STATUS_ONLINE))
        db, _ = promote_env(_node(_device("keep", NVMeDevice.STATUS_ONLINE)), after, lvols=[lvol])

        storage_node_ops.make_sec_new_primary(NODE_ID)
        assert after.primary_ip == "10.244.5.188"
        assert db.written == [after]
        assert lvol.hostname == "worker-4" and lvol.written

    def test_a_second_promote_has_nothing_to_do_and_succeeds(self, promote_env):
        settled = _node(_device("new-1", NVMeDevice.STATUS_ONLINE),
                        _device("old-1", NVMeDevice.STATUS_FAILED_AND_MIGRATED))
        _, calls = promote_env(settled, settled)
        assert storage_node_ops.make_sec_new_primary(NODE_ID) is True
        assert calls == []


class TestAnUnsettledNodeIsReported:
    def test_an_origin_device_left_removed_fails_the_promote(self, promote_env):
        """The lab case: the retirement was refused and the record still says
        removed. The promote must not answer True over it."""
        before = _node(_device("old-1", NVMeDevice.STATUS_REMOVED))
        after = _node(_device("old-1", NVMeDevice.STATUS_REMOVED))
        promote_env(before, after, fail_ok=False)

        with pytest.raises(PromoteIncompleteError) as err:
            storage_node_ops.make_sec_new_primary(NODE_ID)
        assert err.value.still_removed == ["old-1"]
        assert "old-1" in str(err.value)
        assert "device_set_failed(old-1)" in str(err.value)

    def test_a_new_device_left_new_fails_the_promote(self, promote_env):
        before = _node(_device("new-1", NVMeDevice.STATUS_NEW))
        after = _node(_device("new-1", NVMeDevice.STATUS_NEW))
        promote_env(before, after, add_ok=False)

        with pytest.raises(PromoteIncompleteError) as err:
            storage_node_ops.make_sec_new_primary(NODE_ID)
        assert err.value.still_new == ["new-1"]

    def test_the_record_is_the_verdict_not_the_calls_answer(self, promote_env):
        """A call that answered True but did not change the record is still
        unfinished work; a call that answered False after which the record
        is settled is not."""
        before = _node(_device("old-1", NVMeDevice.STATUS_REMOVED))
        promote_env(before, _node(_device("old-1", NVMeDevice.STATUS_REMOVED)), fail_ok=True)
        with pytest.raises(PromoteIncompleteError):
            storage_node_ops.make_sec_new_primary(NODE_ID)

        before = _node(_device("old-1", NVMeDevice.STATUS_REMOVED))
        promote_env(before, _node(_device("old-1", NVMeDevice.STATUS_FAILED)), fail_ok=False)
        assert storage_node_ops.make_sec_new_primary(NODE_ID) is True

    def test_the_node_is_re_pointed_even_when_the_promote_fails(self, promote_env):
        """The node is on the new host either way; a volume naming the old
        host would be wrong on every retry."""
        lvol = _Lvol()
        after = _node(_device("old-1", NVMeDevice.STATUS_REMOVED))
        db, _ = promote_env(_node(_device("old-1", NVMeDevice.STATUS_REMOVED)), after,
                            fail_ok=False, lvols=[lvol])
        with pytest.raises(PromoteIncompleteError):
            storage_node_ops.make_sec_new_primary(NODE_ID)
        assert after.primary_ip == "10.244.5.188" and db.written == [after]
        assert lvol.hostname == "worker-4"
