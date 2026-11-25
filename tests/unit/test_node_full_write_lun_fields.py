"""A full-object write of a StorageNode keeps the stored multi-attach LUN
ownership fields of its devices (serving_node_id, fenced_node_ids,
array_attached_node_ids): only atomic_update changes them.

2026-11-20 FTT2 bed (net cut sn0 + SPDK kill sn1): the reintegration
unfenced a node at 21:32:37.754; the owner's restart, holding a copy loaded
before that, wrote its full record five times in the next 1.4 s and restored
fenced_node_ids. The node stayed fenced and the case never healed."""

import json
import unittest

from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _node(fenced, serving="", attached=(), status=StorageNode.STATUS_ONLINE, multi=True):
    n = StorageNode()
    n.uuid = "owner"
    n.status = status
    d = NVMeDevice()
    d.uuid = "dev-1"
    d.node_id = "owner"
    d.multi_attach = multi
    d.fenced_node_ids = list(fenced)
    d.serving_node_id = serving
    d.array_attached_node_ids = list(attached)
    n.nvme_devices = [d]
    return n


class _Tr:
    def __init__(self, stored):
        self.data = {b"k": json.dumps(stored.to_dict(unwrap_secrets=True)).encode()} if stored else {}

    def get(self, key):
        raw = self.data.get(key)

        class _V:
            def wait(self_inner):
                return self_inner

            def present(self_inner):
                return raw is not None

            def __bytes__(self_inner):
                return raw
        return _V()

    def set(self, key, value):
        self.data[key] = value


def _write(stored, stale):
    tr = _Tr(stored)
    BaseModel._write_tx(tr, b"k", json.dumps(stale.to_dict(unwrap_secrets=True)).encode(),
                        StorageNode, stale, [], None, None)
    return StorageNode().from_dict(json.loads(tr.data[b"k"])).nvme_devices[0]


class TestFullWriteKeepsLunOwnership(unittest.TestCase):

    def test_stale_copy_does_not_refence(self):
        stored = _node(fenced=[], serving="node-b", attached=["node-a", "node-b"])
        stale = _node(fenced=["node-a"], serving="node-b", attached=["node-b"],
                      status=StorageNode.STATUS_RESTARTING)
        dev = _write(stored, stale)
        self.assertEqual(dev.fenced_node_ids, [])
        self.assertEqual(dev.array_attached_node_ids, ["node-a", "node-b"])

    def test_stale_copy_does_not_undo_a_takeover(self):
        dev = _write(_node(fenced=["owner"], serving="node-b"), _node(fenced=[], serving=""))
        self.assertEqual(dev.serving_node_id, "node-b")
        self.assertEqual(dev.fenced_node_ids, ["owner"])

    def test_other_fields_are_written(self):
        stored = _node(fenced=[])
        stale = _node(fenced=[], status=StorageNode.STATUS_RESTARTING)
        stale.nvme_devices[0].nvmf_nqn = "nqn.new"
        tr = _Tr(stored)
        BaseModel._write_tx(tr, b"k", json.dumps(stale.to_dict(unwrap_secrets=True)).encode(),
                            StorageNode, stale, [], None, None)
        n = StorageNode().from_dict(json.loads(tr.data[b"k"]))
        self.assertEqual(n.status, StorageNode.STATUS_RESTARTING)
        self.assertEqual(n.nvme_devices[0].nvmf_nqn, "nqn.new")

    def test_new_record_is_written_as_is(self):
        dev = _write(None, _node(fenced=["x"], serving="y"))
        self.assertEqual((dev.fenced_node_ids, dev.serving_node_id), (["x"], "y"))

    def test_regular_node_is_untouched(self):
        # No multi-attach device: no stored-record read, the write as before.
        stored, stale = _node(fenced=[], multi=False), _node(fenced=["x"], multi=False)
        self.assertFalse(stale.needs_full_write_merge())
        tr = _Tr(stored)
        reads = []
        get = tr.get
        tr.get = lambda key: (reads.append(key), get(key))[1]
        BaseModel._write_tx(tr, b"k", json.dumps(stale.to_dict(unwrap_secrets=True)).encode(),
                            StorageNode, stale, [], None, None)
        self.assertEqual(reads, [])
        dev = StorageNode().from_dict(json.loads(tr.data[b"k"])).nvme_devices[0]
        self.assertEqual(dev.fenced_node_ids, ["x"])


if __name__ == "__main__":
    unittest.main()
