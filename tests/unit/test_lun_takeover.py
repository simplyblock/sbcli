"""Multi-attach array LUN mode, phase 3: LUN takeover by a standby node.

  - array plugin interface: registry, entry points, the fake is never
    registered by default.
  - controllers/lun_takeover: fence first, standby choice, open-only alceml,
    export under the same NQN/namespace, record, paths; fence failure, no
    plugin, no readable standby, idempotent re-run, concurrent takeover.
  - storage_node_monitor: takeovers on node loss (multi-attach only),
    unavailable on failure, regular clusters unchanged.
  - storage_node_ops: owner restart skips LUNs served elsewhere; discovery
    never adopts (formats) another node's LUN; connect uses the serving node.
  - device_controller: ONLINE while the owner is down only via a takeover.
"""

import copy
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import array, constants, multi_attach, storage_node_ops, utils
from simplyblock_core.array.fake import FakeArrayPlugin
from simplyblock_core.controllers import device_controller, lun_takeover
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor

DEV_ID = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"
WWN = "0x600a098038314b6f2b5d4e6a4f2d6b31"
BY_ID = "/dev/disk/by-id/wwn-0x600a098038314b6f2b5d4e6a4f2d6b31"
SIZE = 8 * 1024 ** 3
PLUGIN = "fake-test"


# ------------------------------------------------------------------ fakes

class FakeSpdk:
    """The RPC surface a takeover uses, with bdev/subsystem state."""

    def __init__(self, name):
        self.name = name
        self.bdevs = {}
        self.subsystems = {}
        self.calls = []
        self.fail = set()
        self.down = False

    def get_version(self):
        if self.down:
            raise ConnectionError("Could not reach remote")
        return {"version": "fake"}

    def _rec(self, op, *args, **kwargs):
        self.calls.append((op, args, kwargs))
        return op not in self.fail

    def ops(self, op):
        return [c for c in self.calls if c[0] == op]

    def bdev_get(self, name):
        return copy.deepcopy(self.bdevs.get(name))

    def bdev_aio_create(self, name, filename, block_size=0):
        if not self._rec("bdev_aio_create", name, filename, block_size):
            return None
        self.bdevs[name] = {"name": name, "block_size": 4096, "num_blocks": SIZE // 4096}
        return name

    def bdev_alceml_create(self, name, base, uuid, **kwargs):
        if not self._rec("bdev_alceml_create", name, base, uuid, **kwargs):
            return None
        self.bdevs[name] = {"name": name}
        return name

    def bdev_PT_NoExcl_create(self, name, base):
        self._rec("bdev_PT_NoExcl_create", name, base)
        self.bdevs[name] = {"name": name, "aliases": [f"pt-uuid-{self.name}"]}
        return True

    def subsystem_get(self, nqn):
        return copy.deepcopy(self.subsystems.get(nqn))

    def subsystem_create(self, nqn, sn, mn, **kwargs):
        self._rec("subsystem_create", nqn, sn, mn, **kwargs)
        self.subsystems[nqn] = {"nqn": nqn, "listen_addresses": [], "namespaces": []}
        return True

    def listeners_create(self, nqn, trtype, traddr, port):
        self._rec("listeners_create", nqn, trtype, traddr, port)
        self.subsystems[nqn]["listen_addresses"].append({"traddr": traddr})
        return True

    def nvmf_subsystem_add_ns(self, nqn, pt, uuid=None, nguid=None):
        if not self._rec("nvmf_subsystem_add_ns", nqn, pt, uuid):
            return None
        self.subsystems[nqn]["namespaces"].append({"name": pt, "uuid": uuid, "nguid": nguid})
        return 1

    def alceml_set_qos_weights(self, weights):
        return True

    def bdev_nvme_attach_controller(self, *args, **kwargs):
        return self._rec("attach", *args, **kwargs)

    def bdev_nvme_detach_controller(self, *args, **kwargs):
        return self._rec("detach", *args, **kwargs)

    def bdev_nvme_set_multipath_policy(self, *args, **kwargs):
        return self._rec("policy", *args, **kwargs)

    def subsystem_delete(self, nqn):
        self._rec("subsystem_delete", nqn)
        self.subsystems.pop(nqn, None)

    def bdev_PT_NoExcl_delete(self, name):
        self._rec("bdev_PT_NoExcl_delete", name)
        self.bdevs.pop(name, None)

    def bdev_alceml_delete(self, name):
        self._rec("bdev_alceml_delete", name)
        self.bdevs.pop(name, None)

    def bdev_aio_delete(self, name):
        self._rec("bdev_aio_delete", name)
        self.bdevs.pop(name, None)


class FakeDB:
    def __init__(self, cluster, nodes):
        self.cluster = cluster
        self.nodes = {n.get_id(): n for n in nodes}
        self.lock_holder = None

    def get_cluster_by_id(self, _cid):
        return self.cluster

    def get_storage_nodes_by_cluster_id(self, _cid):
        return list(self.nodes.values())

    def get_storage_node_by_id(self, nid):
        if nid not in self.nodes:
            raise KeyError(nid)
        return self.nodes[nid]

    def get_storage_device_by_id(self, did):
        for n in self.nodes.values():
            for d in n.nvme_devices:
                if d.get_id() == did:
                    return d
        raise KeyError(did)

    def atomic_update(self, obj, fn):
        fresh = self.nodes[obj.get_id()]
        fn(fresh)
        return fresh

    def acquire_lun_takeover_lock(self, _did, owner, _ttl):
        if self.lock_holder and self.lock_holder != owner:
            return False, self.lock_holder
        self.lock_holder = owner
        return True, None

    def release_lun_takeover_lock(self, _did, owner):
        if self.lock_holder == owner:
            self.lock_holder = None


def _cluster(plugin=PLUGIN, multi=True):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.nqn = f"{constants.CLUSTER_NQN}:cluster-1"
    cl.distr_ndcs, cl.distr_npcs = 1, 0
    cl.multi_attach = multi
    cl.array_plugin = plugin
    cl.status = Cluster.STATUS_ACTIVE
    return cl


def _node(nid, ip, block, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = nid
    n.cluster_id = "cluster-1"
    n.hostname = f"host-{nid}_5260"
    n.rpc_port = 5260
    n.status = status
    n.nvmf_port = 4420
    n.cntlid_block = block
    n.active_tcp = True
    nic = IFace()
    nic.ip4_address = ip
    n.data_nics = [nic]
    n.spdk = FakeSpdk(nid)
    n.rpc_client = lambda *a, _s=n.spdk, **k: _s
    n.agent = MagicMock()
    n.client = lambda *a, _c=n.agent, **k: _c
    return n


def _lun_entry(signature=True, path=BY_ID, size=SIZE):
    return {"name": "sdc", "device_path": "/dev/sdc", "by_id_path": path,
            "by_id_paths": [path], "size": size, "has_alceml_signature": signature}


def _bed(plugin=PLUGIN, multi=True, dev_status=NVMeDevice.STATUS_ONLINE):
    """Owner A (serves its LUN), secondary B, tertiary C, peer D."""
    a = _node("node-a", "10.0.0.1", 0)
    b = _node("node-b", "10.0.0.2", 1)
    c = _node("node-c", "10.0.0.3", 2)
    d = _node("node-d", "10.0.0.4", 3)
    a.secondary_node_id, a.tertiary_node_id = "node-b", "node-c"
    dev = NVMeDevice()
    dev.uuid = DEV_ID
    dev.node_id = "node-a"
    dev.cluster_id = "cluster-1"
    dev.status = dev_status
    dev.size = SIZE
    dev.bdev_type = "aio"
    dev.serial_number = WWN
    dev.multi_attach = multi
    dev.lun_wwn = WWN
    dev.alceml_bdev = dev.alceml_name = f"alceml_{DEV_ID}"
    dev.pt_bdev = f"alceml_{DEV_ID}_PT"
    dev.nvmf_nqn = multi_attach.device_nqn(f"{constants.CLUSTER_NQN}:cluster-1", DEV_ID)
    dev.nvmf_ip = "10.0.0.1"
    dev.nvmf_port = 4420
    dev.node_paths = {"node-a": {"path": BY_ID, "bdev": "aio_x"}}
    a.nvme_devices = [dev]
    for n in (b, c, d):
        n.agent.lun_info.return_value = ([_lun_entry()], None)
    a.status = StorageNode.STATUS_OFFLINE
    return FakeDB(_cluster(plugin, multi), [a, b, c, d]), dev


class _TakeoverCase(unittest.TestCase):

    def setUp(self):
        FakeArrayPlugin.reset()
        array.register_plugin(PLUGIN, FakeArrayPlugin)
        self.addCleanup(array.unregister_plugin, PLUGIN)
        array.forget_plugin()
        self.addCleanup(array.forget_plugin)
        self.events = []
        for target, attr, kw in (
                (lun_takeover, "DBController", {}),
                (lun_takeover.ec, "log_event_cluster",
                 {"side_effect": lambda **k: self.events.append(k)}),
                (device_controller, "device_set_state", {"return_value": True})):
            p = patch.object(target, attr, **kw)
            setattr(self, f"m_{attr}", p.start())
            self.addCleanup(p.stop)
        qos = patch("simplyblock_core.controllers.qos_controller.get_qos_weights_list", return_value=[])
        qos.start()
        self.addCleanup(qos.stop)

    def run_takeover(self, db, failed="node-a", **kw):
        self.m_DBController.return_value = db
        return lun_takeover.take_over_lun_outcome(DEV_ID, failed, "test", **kw)

    def dev(self, db):
        return db.get_storage_device_by_id(DEV_ID)


# ------------------------------------------------------------ the takeover

class TestTakeover(_TakeoverCase):

    def test_happy_path(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        # 1. fence of the failed node, by hostname, first.
        self.assertEqual(FakeArrayPlugin.calls, [("fence", WWN, "host-node-a")])
        b = db.nodes["node-b"]
        # 4. open-only alceml on the secondary, over the by-id path.
        aio = b.spdk.ops("bdev_aio_create")
        self.assertEqual(aio[0][1][1], BY_ID)
        alceml = b.spdk.ops("bdev_alceml_create")
        self.assertEqual(len(alceml), 1)
        self.assertEqual(alceml[0][2]["pba_init_mode"], 1)
        self.assertEqual(alceml[0][2]["stuck_io_timeout_ms"], constants.MULTI_ATTACH_ALCEML_STUCK_IO_MS)
        nqn = multi_attach.device_nqn(db.cluster.nqn, DEV_ID)
        sub = b.spdk.ops("subsystem_create")[0]
        self.assertEqual(sub[1][0], nqn)
        self.assertEqual((sub[2]["min_cntlid"], sub[2]["max_cntlid"]), multi_attach.cntlid_range(1))
        ns = b.spdk.ops("nvmf_subsystem_add_ns")[0]
        self.assertEqual(ns[1][2], multi_attach.namespace_uuid(self.dev(db)))
        self.assertEqual(b.spdk.ops("listeners_create")[0][1][2], "10.0.0.2")
        # 6. record.
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-b")
        self.assertEqual(dev.nvmf_ip, "10.0.0.2")
        self.assertEqual(dev.node_paths["node-b"]["path"], BY_ID)
        self.assertIn("node-a", dev.fenced_node_ids)
        self.assertEqual(dev.status, NVMeDevice.STATUS_ONLINE)
        self.m_device_set_state.assert_not_called()
        # 5 + 7. new path on every ONLINE node (standby's loopback too), old one off.
        for nid in ("node-b", "node-c", "node-d"):
            spdk = db.nodes[nid].spdk
            att = spdk.ops("attach")
            self.assertEqual(len(att), 1, nid)
            self.assertEqual(att[0][1][:5], (f"remote_alceml_{DEV_ID}", nqn, "10.0.0.2", 4420, "TCP"))
            self.assertEqual(att[0][2]["multipath"], "multipath")
            self.assertEqual(att[0][2]["ctrlr_loss_timeout_sec"], constants.MULTI_ATTACH_HOLD_SEC)
            self.assertEqual(spdk.ops("policy")[0][1], (f"remote_alceml_{DEV_ID}n1", "active_passive"))
            det = spdk.ops("detach")
            self.assertEqual(det[0][2]["traddr"], "10.0.0.1")
        self.assertEqual(db.nodes["node-a"].spdk.calls, [])
        self.assertTrue(any("taken over" in e["message"] for e in self.events))

    def test_never_formats(self):
        db, _ = _bed()
        self.run_takeover(db)
        for n in db.nodes.values():
            for call in n.spdk.ops("bdev_alceml_create"):
                self.assertEqual(call[2]["pba_init_mode"], lun_takeover.ALCEML_OPEN_ONLY)

    def test_fence_failure_means_no_takeover(self):
        for mode in ("fail", "fail_falsy"):
            with self.subTest(mode=mode):
                FakeArrayPlugin.reset()
                getattr(FakeArrayPlugin, mode).add("fence")
                db, _ = _bed()
                self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
                self.assertEqual(self.dev(db).serving_node(), "node-a")
                # The standby may have been probed (read-only, during the
                # fence); nothing was built on any node.
                for n in db.nodes.values():
                    self.assertEqual(n.spdk.calls, [])

    def test_no_plugin_refuses(self):
        db, _ = _bed(plugin="")
        self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.assertEqual(self.dev(db).serving_node(), "node-a")
        self.assertTrue(all(n.spdk.calls == [] for n in db.nodes.values()))
        self.assertTrue(any("no array plugin" in e["message"] for e in self.events))

    def test_unknown_plugin_refuses(self):
        db, _ = _bed(plugin="no-such-plugin")
        self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.assertEqual(FakeArrayPlugin.calls, [])

    def test_no_standby_can_read_holds(self):
        db, _ = _bed()
        for nid in ("node-b", "node-c"):
            db.nodes[nid].agent.lun_info.return_value = ([_lun_entry(signature=None)], None)
        self.assertEqual(self.run_takeover(db), lun_takeover.NO_STANDBY)
        self.assertEqual(self.dev(db).serving_node(), "node-a")
        self.assertTrue(all(n.spdk.calls == [] for n in db.nodes.values()))
        # The node that is not owner/secondary/tertiary is never a candidate.
        db.nodes["node-d"].agent.lun_info.assert_not_called()
        self.assertTrue(any("no standby can read" in e["message"] for e in self.events))

    def test_secondary_offline_uses_tertiary(self):
        db, _ = _bed()
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")
        self.assertEqual(self.dev(db).nvmf_ip, "10.0.0.3")
        self.assertEqual(db.nodes["node-b"].spdk.calls, [])

    def test_secondary_unreadable_uses_tertiary(self):
        db, _ = _bed()
        db.nodes["node-b"].agent.lun_info.return_value = ([], None)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")

    def test_rerun_is_idempotent(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        for n in db.nodes.values():
            n.spdk.calls.clear()
        FakeArrayPlugin.calls.clear()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(FakeArrayPlugin.calls, [])
        b = db.nodes["node-b"]
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])
        self.assertEqual(b.spdk.ops("subsystem_create"), [])
        self.assertEqual(self.dev(db).serving_node(), "node-b")
        self.assertTrue(b.spdk.ops("attach"))

    def test_path_switch_marks_the_device_online_on_every_node(self):
        # A node told "unavailable" during the gap must not keep it (1+0:
        # every read there would fail).
        from simplyblock_core import distr_controller
        db, _ = _bed()
        with patch.object(distr_controller, "send_dev_status_event") as sent:
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        targets = sorted(c.args[2].get_id() for c in sent.call_args_list
                         if c.args[1] == NVMeDevice.STATUS_ONLINE)
        self.assertEqual(targets, sorted(n for n, x in db.nodes.items()
                                         if n != "node-a" and x.status == StorageNode.STATUS_ONLINE))

    def test_rerun_after_partial_export_reuses_it(self):
        db, _ = _bed()
        standbys = [db.nodes[n] for n in ("node-b", "node-c", "node-d")]
        for n in standbys:
            n.spdk.fail.add("nvmf_subsystem_add_ns")
        self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        # Cleanup closed what each attempt opened; nothing recorded.
        self.assertEqual(self.dev(db).serving_node(), "node-a")
        b = standbys[0]
        self.assertNotIn(f"alceml_{DEV_ID}", b.spdk.bdevs)
        self.assertTrue(b.spdk.ops("bdev_alceml_delete"))
        for n in standbys:
            n.spdk.fail.clear()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-b")

    def test_failed_export_moves_to_the_next_standby(self):
        db, _ = _bed()
        db.nodes["node-b"].spdk.fail.add("nvmf_subsystem_add_ns")
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")
        # node-b was retried once (it answers), then left; its stack closed.
        self.assertEqual(len(db.nodes["node-b"].spdk.ops("bdev_alceml_create")), 2)
        self.assertNotIn(f"alceml_{DEV_ID}", db.nodes["node-b"].spdk.bdevs)

    def test_standby_is_chosen_while_the_fence_runs(self):
        import threading
        db, _ = _bed()
        probed = threading.Event()
        db.nodes["node-b"].agent.lun_info.side_effect = (
            lambda *_a, **_k: (probed.set(), ([_lun_entry()], None))[1])
        seen = []

        class SlowFence(FakeArrayPlugin):
            def fence(self, lun_id, host_id):
                seen.append(probed.wait(5))
                return super().fence(lun_id, host_id)

        array.register_plugin(PLUGIN, SlowFence)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(seen, [True])

    def test_nothing_is_built_before_the_fence_is_acknowledged(self):
        db, _ = _bed()
        order = []
        b = db.nodes["node-b"]
        create = b.spdk.bdev_aio_create
        b.spdk.bdev_aio_create = lambda *a, **k: (order.append("build"), create(*a, **k))[1]

        class Fence(FakeArrayPlugin):
            def fence(self, lun_id, host_id):
                order.append("fence")
                return super().fence(lun_id, host_id)

        array.register_plugin(PLUGIN, Fence)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(order, ["fence", "build"])

    def test_array_connection_is_reused(self):
        db, _ = _bed()
        self.run_takeover(db)
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        self.assertEqual(self.run_takeover(db, failed="node-b"), lun_takeover.TAKEN_OVER)
        self.assertEqual(len(FakeArrayPlugin.connected), 1)

    def test_fence_failure_retried_on_a_new_connection(self):
        db, _ = _bed()
        stale, fresh = MagicMock(), MagicMock()
        stale.fence.side_effect = RuntimeError("ExpiredToken")
        fresh.fence.return_value = True
        with patch.object(array, "cached_plugin_for_cluster", side_effect=[stale, fresh]),                 patch.object(array, "forget_plugin") as forget:
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        forget.assert_called_once_with("cluster-1")
        fresh.fence.assert_called_once_with(WWN, "host-node-a")

    def test_concurrent_takeover_is_refused(self):
        db, _ = _bed()
        db.lock_holder = "another-runner"
        self.assertEqual(self.run_takeover(db), lun_takeover.IN_PROGRESS)
        self.assertEqual(FakeArrayPlugin.calls, [])

    def test_failed_standby_is_taken_over_again(self):
        db, _ = _bed()
        self.run_takeover(db)
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        FakeArrayPlugin.calls.clear()
        self.assertEqual(self.run_takeover(db, failed="node-b"), lun_takeover.TAKEN_OVER)
        self.assertEqual(FakeArrayPlugin.calls, [("fence", WWN, "host-node-b")])
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-c")
        self.assertEqual(sorted(dev.fenced_node_ids), ["node-a", "node-b"])

    def test_unavailable_device_comes_back_online(self):
        db, _ = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.m_device_set_state.assert_called_once_with(
            DEV_ID, NVMeDevice.STATUS_ONLINE, cause=device_controller.CAUSE_LUN_TAKEOVER,
            connect_peers=False)

    def test_unfence_node(self):
        db, _ = _bed()
        self.run_takeover(db)
        self.assertTrue(lun_takeover.unfence_node(DEV_ID, "node-a"))
        self.assertEqual(FakeArrayPlugin.calls[-1], ("unfence", WWN, "host-node-a"))
        self.assertEqual(self.dev(db).fenced_node_ids, [])
        # The serving node is never "unfenced" (it was never fenced).
        self.assertFalse(lun_takeover.unfence_node(DEV_ID, "node-b"))

    def test_regular_device_is_refused(self):
        db, _ = _bed(multi=False)
        self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.assertEqual(FakeArrayPlugin.calls, [])

    def test_size_mismatch_is_refused(self):
        db, dev = _bed()
        dev.size = SIZE * 2
        for nid in ("node-b", "node-c"):
            db.nodes[nid].agent.lun_info.return_value = ([_lun_entry(size=SIZE * 2)], None)
        self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.assertEqual(self.dev(db).serving_node(), "node-a")


# ----------------------------------------------------------- helpers/array

class TestHelpers(unittest.TestCase):

    def test_pick_lun_path(self):
        dev = NVMeDevice()
        dev.node_id = "a"
        dev.size = SIZE
        dev.node_paths = {"a": {"path": BY_ID}}
        self.assertEqual(multi_attach.pick_lun_path([_lun_entry()], dev), (BY_ID, ""))
        self.assertIsNone(multi_attach.pick_lun_path([], dev)[0])
        self.assertIsNone(multi_attach.pick_lun_path([_lun_entry(signature=False)], dev)[0])
        self.assertIsNone(multi_attach.pick_lun_path([_lun_entry(signature=None)], dev)[0])
        other = _lun_entry(path="/dev/disk/by-id/scsi-x")
        self.assertIsNone(multi_attach.pick_lun_path([other, dict(other, name="sdd")], dev)[0])
        self.assertEqual(multi_attach.pick_lun_path([other, _lun_entry()], dev)[0], BY_ID)

    def test_array_host_and_candidates(self):
        n = StorageNode()
        n.uuid = "a"
        n.hostname, n.rpc_port = "worker-1_5260", 5260
        self.assertEqual(multi_attach.array_host(n), "worker-1")
        n.secondary_node_id, n.tertiary_node_id = "b", "c"
        self.assertEqual(multi_attach.standby_candidates(n, "a"), ["b", "c"])
        self.assertEqual(multi_attach.standby_candidates(n, "b"), ["c", "a"])

    def test_fake_plugin_is_not_registered_by_default(self):
        with self.assertRaises(array.ArrayError):
            array.plugin_class("fake")
        with self.assertRaises(array.ArrayNotConfigured):
            array.plugin_for_cluster(_cluster(plugin=""))

    def test_entry_point_loading(self):
        ep = MagicMock(value="pkg:Cls")
        ep.load.return_value = FakeArrayPlugin
        with patch.object(array.metadata, "entry_points", return_value=[ep]) as eps:
            plugin = array.load_plugin("vendor", "https://array", {"pool": "p"})
        eps.assert_called_once_with(group=array.ENTRY_POINT_GROUP, name="vendor")
        self.assertIsInstance(plugin, FakeArrayPlugin)
        self.assertEqual((plugin.endpoint, plugin.options), ("https://array", {"pool": "p"}))
        ep.load.return_value = object
        with patch.object(array.metadata, "entry_points", return_value=[ep]):
            with self.assertRaises(array.ArrayError):
                array.load_plugin("vendor")

    def test_find_lun_devices(self):
        from simplyblock_web import node_utils
        inv = [{"type": "disk", "name": "sdb", "device_path": "/dev/sdb", "wwn": "0X600A",
                "serial": "s1", "size": 1, "by_id_path": "/x"},
               {"type": "disk", "name": "sdc", "device_path": "/dev/sdc", "wwn": "", "serial": "zz",
                "size": 1, "by_id_path": "/y"}]
        with patch.object(node_utils, "get_block_devices_info", return_value=inv), \
                patch.object(node_utils, "simplyblock_data_signature", return_value=True) as sig:
            out = node_utils.find_lun_devices("600a")
        self.assertEqual([e["name"] for e in out], ["sdb"])
        self.assertTrue(out[0]["has_alceml_signature"])
        sig.assert_called_once_with("sdb")

    def test_find_lun_devices_skips_a_dead_controller(self):
        # A volume detached while the host booted: the kernel keeps a 0-byte
        # namespace of a dead controller.
        from simplyblock_web import node_utils
        inv = [{"type": "disk", "name": "nvme3n1", "device_path": "/dev/nvme3n1", "wwn": "",
                "serial": "vol08dcbe488933595ca", "size": 0, "by_id_path": "/x"}]
        states = {"/sys/block/nvme3n1/device/state": "dead"}
        with patch.object(node_utils, "get_block_devices_info", return_value=inv),                 patch.object(node_utils, "_read_sysfs", side_effect=lambda p: states.get(p, "")),                 patch.object(node_utils, "simplyblock_data_signature", return_value=None):
            self.assertEqual(node_utils.find_lun_devices("vol08dcbe488933595ca"), [])
            states["/sys/block/nvme3n1/device/state"] = "live"
            self.assertEqual(len(node_utils.find_lun_devices("vol08dcbe488933595ca")), 1)

    def test_dead_nvme_controllers_are_removed_and_the_rest_rescanned(self):
        # 2026-11-24 warm FTT2 bed: the dead controller's namespace head kept
        # the volume's name; the reattached volume's namespace was refused.
        import glob

        from simplyblock_web import node_utils
        sysfs = {"/sys/class/nvme/nvme3/state": "dead", "/sys/class/nvme/nvme3/transport": "pcie",
                 "/sys/class/nvme/nvme2/state": "live", "/sys/class/nvme/nvme2/transport": "pcie",
                 "/sys/class/nvme/nvme5/state": "dead", "/sys/class/nvme/nvme5/transport": "tcp"}
        written = []

        def fake_glob(pattern):
            if pattern == "/sys/class/nvme/nvme*":
                return ["/sys/class/nvme/nvme2", "/sys/class/nvme/nvme3", "/sys/class/nvme/nvme5"]
            if pattern == "/sys/class/nvme/nvme*/rescan_controller":
                return ["/sys/class/nvme/nvme2/rescan_controller"]
            return []
        with patch.object(glob, "glob", side_effect=fake_glob), \
                patch.object(node_utils, "_read_sysfs", side_effect=lambda p: sysfs.get(p, "")), \
                patch.object(node_utils.os.path, "realpath",
                             side_effect=lambda p: "/sys/devices/pci0000:00/0000:00:1f.0"
                             if p.startswith("/sys/class/nvme/nvme3") else "/x"), \
                patch.object(node_utils.os.path, "exists", return_value=True), \
                patch.object(node_utils, "_write_sysfs", side_effect=lambda p, v: written.append((p, v))):
            removed = node_utils.remove_dead_nvme_controllers()
        self.assertEqual(removed, ["0000:00:1f.0"])
        self.assertEqual(written, [("/sys/devices/pci0000:00/0000:00:1f.0/remove", "1"),
                                   ("/sys/class/nvme/nvme2/rescan_controller", "1")])


# ------------------------------------------------------------------ monitor

class TestMonitorHook(unittest.TestCase):

    def _run(self, db):
        started = []
        with patch.object(storage_node_monitor, "db", db), \
                patch.object(storage_node_monitor, "_start_lun_takeover",
                             side_effect=lambda did, nid, why: started.append((did, nid))), \
                patch.object(storage_node_monitor.device_controller,
                             "device_set_unavailable") as unavailable:
            storage_node_monitor._set_node_devices_unavailable(db.nodes["node-a"], "test")
        return started, [c.args[0] for c in unavailable.call_args_list]

    def test_multi_attach_starts_takeover_instead_of_unavailable(self):
        db, dev = _bed()
        plain = NVMeDevice()
        plain.uuid, plain.node_id, plain.status = "plain", "node-a", NVMeDevice.STATUS_ONLINE
        db.nodes["node-a"].nvme_devices.append(plain)
        started, unavailable = self._run(db)
        self.assertEqual(started, [(DEV_ID, "node-a")])
        self.assertEqual(unavailable, ["plain"])

    def test_lun_served_elsewhere_is_untouched_and_taken_over_luns_follow(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        foreign = copy.deepcopy(dev)
        foreign.uuid, foreign.node_id, foreign.serving_node_id = "foreign", "node-c", "node-a"
        db.nodes["node-c"].nvme_devices = [foreign]
        started, unavailable = self._run(db)
        self.assertEqual(started, [("foreign", "node-a")])
        self.assertEqual(unavailable, [])

    def test_regular_cluster_unchanged(self):
        db, dev = _bed(multi=False)
        started, unavailable = self._run(db)
        self.assertEqual(started, [])
        self.assertEqual(unavailable, [DEV_ID])

    def test_worker_marks_unavailable_on_failure_only(self):
        db, dev = _bed()
        for outcome, expected in ((lun_takeover.FAILED, [DEV_ID]),
                                  (lun_takeover.TAKEN_OVER, []),
                                  (lun_takeover.IN_PROGRESS, [])):
            with self.subTest(outcome=outcome), \
                    patch.object(storage_node_monitor, "db", db), \
                    patch.object(lun_takeover, "take_over_lun_outcome", return_value=outcome), \
                    patch.object(storage_node_monitor.device_controller,
                                 "device_set_unavailable") as unavailable:
                storage_node_monitor._lun_takeover_worker(DEV_ID, "node-a", "t", 0.0)
                self.assertEqual([c.args[0] for c in unavailable.call_args_list], expected)

    def test_worker_holds_until_the_hold_expires(self):
        db, dev = _bed()
        with patch.object(storage_node_monitor, "db", db), \
                patch.object(lun_takeover, "take_over_lun_outcome", return_value=lun_takeover.NO_STANDBY), \
                patch.object(storage_node_monitor.time, "monotonic", return_value=100.0), \
                patch.object(storage_node_monitor.time, "sleep") as sleep, \
                patch.object(storage_node_monitor.device_controller,
                             "device_set_unavailable") as unavailable:
            storage_node_monitor._lun_takeover_worker(DEV_ID, "node-a", "t", 90.0)
        sleep.assert_called_once_with(90.0 + constants.MULTI_ATTACH_HOLD_SEC - 100.0)
        unavailable.assert_called_once_with(DEV_ID)


# ----------------------------------------------------- owner restart/discovery

class TestOwnerReturn(unittest.TestCase):

    def test_restart_skips_luns_served_elsewhere(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        own = copy.deepcopy(dev)
        own.uuid, own.serving_node_id = "own-served", ""
        a = db.nodes["node-a"]
        a.nvme_devices = [dev, own]
        a.jm_device = None
        built = []
        with patch.object(storage_node_ops, "DBController"), \
                patch.object(storage_node_ops, "_create_storage_device_stack",
                             side_effect=lambda rpc, nvme, sn, ar: built.append(nvme.get_id()) or nvme), \
                patch.object(StorageNode, "write_to_db"):
            self.assertTrue(storage_node_ops._prepare_cluster_devices_on_restart(a))
        self.assertEqual(built, ["own-served"])
        self.assertEqual([d.get_id() for d in a.nvme_devices], [DEV_ID, "own-served"])

    def test_restart_skips_luns_it_is_fenced_from(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        dev.fenced_node_ids = ["node-a"]
        a = db.nodes["node-a"]
        a.jm_device = None
        with patch.object(storage_node_ops, "DBController"),                 patch.object(storage_node_ops, "_create_storage_device_stack") as build,                 patch.object(StorageNode, "write_to_db"):
            self.assertTrue(storage_node_ops._prepare_cluster_devices_on_restart(a))
        build.assert_not_called()

    def _reconcile(self, db, node, found):
        with patch.object(storage_node_ops, "DBController", return_value=db):
            return storage_node_ops._reconcile_discovered_devices(node, found)

    def _found(self, serial, wwn=""):
        d = NVMeDevice()
        d.uuid, d.serial_number, d.lun_wwn = f"new-{serial}", serial, wwn
        d.device_name = "sdx"
        return d

    def test_discovery_never_adopts_another_nodes_lun(self):
        db, dev = _bed()
        b = db.nodes["node-b"]
        b.nvme_devices = []
        new = self._reconcile(db, b, [self._found(WWN.upper(), WWN), self._found("own-lun")])
        self.assertEqual([d.serial_number for d in new], ["own-lun"])
        self.assertNotIn(WWN.upper(), [d.serial_number for d in b.nvme_devices])

    def test_missing_lun_served_elsewhere_is_kept(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        a = db.nodes["node-a"]
        self.assertEqual(self._reconcile(db, a, []), [])
        self.assertEqual(dev.status, NVMeDevice.STATUS_ONLINE)

    def test_regular_cluster_discovery_unchanged(self):
        db, dev = _bed(multi=False)
        b = db.nodes["node-b"]
        b.nvme_devices = []
        new = self._reconcile(db, b, [self._found(WWN, WWN)])
        self.assertEqual(len(new), 1)
        a = db.nodes["node-a"]
        self.assertEqual(self._reconcile(db, a, []), [])
        self.assertEqual(dev.status, NVMeDevice.STATUS_REMOVED)

    def test_add_node_drops_foreign_luns(self):
        db, dev = _bed()
        with patch.object(storage_node_ops, "DBController", return_value=db):
            kept = storage_node_ops.drop_foreign_luns(db.nodes["node-b"], [self._found(WWN, WWN),
                                                                           self._found("mine")])
        self.assertEqual([d.serial_number for d in kept], ["mine"])


# --------------------------------------------------------- connect / state

class TestConnectAndState(unittest.TestCase):

    def test_connect_uses_the_serving_node(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        dev.nvmf_ip = "10.0.0.2"
        db.nodes["node-a"].active_tcp = False      # the dead owner: must not matter
        db.nodes["node-b"].active_rdma = True
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = []
        rpc.bdev_get.return_value = {"name": "b"}
        attach = MagicMock()
        attach.bdev_nvme_attach_controller.return_value = [f"remote_alceml_{DEV_ID}n1"]
        with patch.object(storage_node_ops, "DBController", return_value=db):
            storage_node_ops._connect_device_attach(
                f"remote_alceml_{DEV_ID}", dev, db.nodes["node-d"], rpc, attach, ["10.0.0.2"], True)
        call = attach.bdev_nvme_attach_controller.call_args
        self.assertEqual(call.args[2:5], ("10.0.0.2", 4420, "RDMA"))
        rpc.bdev_nvme_set_multipath_policy.assert_called_once_with(
            f"remote_alceml_{DEV_ID}n1", "active_passive")

    def test_connect_adds_the_missing_path_to_a_held_controller(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        dev.nvmf_ip = "10.0.0.2"
        held = [{"ctrlrs": [{"state": "resetting", "trid": {"traddr": "10.0.0.1"}}]}]
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = held
        rpc.bdev_get.return_value = {"name": "b"}
        attach = MagicMock()
        attach.bdev_nvme_attach_controller.return_value = True
        with patch.object(storage_node_ops, "DBController", return_value=db), \
                patch.object(storage_node_ops.time, "sleep") as sleep:
            out = storage_node_ops._connect_device_attach(
                f"remote_alceml_{DEV_ID}", dev, db.nodes["node-d"], rpc, attach, ["10.0.0.2"], True)
        sleep.assert_not_called()
        self.assertEqual(out, f"remote_alceml_{DEV_ID}n1")
        self.assertEqual(attach.bdev_nvme_attach_controller.call_args.args[2], "10.0.0.2")

    def _set_online(self, db, cause):
        with patch.object(device_controller, "DBController", return_value=db), \
                patch.object(device_controller, "_atomic_device_set"), \
                patch.object(device_controller.device_events, "device_status_change"), \
                patch.object(device_controller.distr_controller, "send_dev_status_event"):
            return device_controller.device_set_state(
                DEV_ID, NVMeDevice.STATUS_ONLINE, cause=cause, connect_peers=False)

    def test_online_while_owner_down_only_via_takeover(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        dev.serving_node_id = "node-b"
        self.assertFalse(self._set_online(db, device_controller.CAUSE_OTHER))
        self.assertTrue(self._set_online(db, device_controller.CAUSE_LUN_TAKEOVER))

    def test_never_online_while_the_serving_node_is_down_or_fenced(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        db.nodes["node-a"].status = StorageNode.STATUS_ONLINE
        dev.serving_node_id = "node-b"
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        self.assertFalse(self._set_online(db, device_controller.CAUSE_NODE_RECOVERY))
        dev.serving_node_id = ""
        dev.fenced_node_ids = ["node-a"]
        self.assertFalse(self._set_online(db, device_controller.CAUSE_OTHER))
        dev.fenced_node_ids = []
        self.assertTrue(self._set_online(db, device_controller.CAUSE_OTHER))

    def test_takeover_cause_needs_an_online_serving_node(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        self.assertFalse(self._set_online(db, device_controller.CAUSE_LUN_TAKEOVER))
        dev.serving_node_id = "node-b"
        db.nodes["node-b"].status = StorageNode.STATUS_OFFLINE
        self.assertFalse(self._set_online(db, device_controller.CAUSE_LUN_TAKEOVER))


if __name__ == "__main__":
    unittest.main()


# --------------------------------------------- multi-attach bed findings, F1

class TestUnreachableTrigger(unittest.TestCase):
    """The regular monitor starts the takeover as soon as it sees a node
    UNREACHABLE (backstop of services/lun_watchdog), not when it is declared
    OFFLINE: on the bed the OFFLINE verdict took ~6 min, far past the peers'
    hold. No confirmation wait: the fence makes an early takeover safe."""

    def setUp(self):
        from simplyblock_core.services import storage_node_monitor as mon
        self.mon = mon
        self.db, self.dev = _bed()
        self.db.get_storage_node_by_id("node-a").status = StorageNode.STATUS_UNREACHABLE
        for target, attr, kw in ((mon, "db", {"new": self.db}),
                                 (mon, "_start_lun_takeover", {})):
            p = patch.object(target, attr, **kw)
            started = p.start()
            if attr == "_start_lun_takeover":
                self.start = started
            self.addCleanup(p.stop)

    def test_at_once(self):
        self.mon._take_over_luns_of_unreachable(self.db.get_storage_node_by_id("node-a"))
        self.start.assert_called_once_with(DEV_ID, "node-a", "node unreachable")

    def test_regular_cluster_never(self):
        self.db.cluster.multi_attach = False
        self.mon._take_over_luns_of_unreachable(self.db.get_storage_node_by_id("node-a"))
        self.start.assert_not_called()

    def test_no_confirmation_wait_left(self):
        self.assertFalse(hasattr(constants, "MULTI_ATTACH_UNREACHABLE_TAKEOVER_SEC"))
        self.assertFalse(hasattr(self.mon, "_unreachable_since"))

    def test_hold_covers_detection_and_takeover(self):
        # ~20 s for the regular monitor to see the node unreachable, then the
        # takeover deadline.
        self.assertGreater(constants.MULTI_ATTACH_HOLD_SEC,
                           20 + constants.MULTI_ATTACH_TAKEOVER_DEADLINE_SEC)


class TestRetireStaleLocalStack(unittest.TestCase):
    """A node back ONLINE without a restart drops its stale stack of a LUN
    another node took over: own loopback path first, then subsystem, PT,
    alceml. Nothing is formatted."""

    def _setup(self, served_by="node-b", with_stack=True):
        db, dev = _bed()
        a = db.get_storage_node_by_id("node-a")
        a.status = StorageNode.STATUS_ONLINE
        dev.serving_node_id = served_by
        if with_stack:
            a.spdk.bdevs[dev.alceml_bdev] = {"name": dev.alceml_bdev}
            a.spdk.bdevs[f"{dev.alceml_bdev}_PT"] = {"name": f"{dev.alceml_bdev}_PT"}
        with patch("simplyblock_core.db_controller.DBController", return_value=db):
            retired = lun_takeover.retire_stale_local_stack(a)
        return a, dev, retired

    def test_retires_in_order(self):
        a, dev, retired = self._setup()
        self.assertEqual(retired, [DEV_ID])
        detach = a.spdk.ops("detach")
        self.assertEqual(detach[0][1][0], f"remote_{dev.alceml_bdev}")
        self.assertEqual(detach[0][2]["traddr"], "10.0.0.1")
        order = [c[0] for c in a.spdk.calls if c[0] in ("subsystem_delete", "bdev_PT_NoExcl_delete",
                                                        "bdev_alceml_delete")]
        self.assertEqual(order, ["subsystem_delete", "bdev_PT_NoExcl_delete", "bdev_alceml_delete"])
        self.assertFalse(a.spdk.ops("bdev_alceml_create"))

    def test_nothing_when_still_served_here(self):
        a, _dev, retired = self._setup(served_by="")
        self.assertEqual(retired, [])
        self.assertFalse(a.spdk.ops("subsystem_delete"))

    def test_nothing_without_a_stack(self):
        a, _dev, retired = self._setup(with_stack=False)
        self.assertEqual(retired, [])
        self.assertFalse(a.spdk.ops("detach"))


class TestReintegrateFencedNode(_TakeoverCase):
    """A node a takeover fenced becomes a standby again (unfenced at the array)
    once it is ONLINE, holds no stack of the LUN, and its host shows no disk of
    the LUN; otherwise it stays fenced."""

    def setUp(self):
        super().setUp()
        p = patch.object(lun_takeover, "REINTEGRATE_VISIBLE_WAIT_SEC", 0)
        p.start()
        self.addCleanup(p.stop)

    def _taken_over(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        a = db.get_storage_node_by_id("node-a")
        a.status = StorageNode.STATUS_ONLINE
        a.agent.lun_info.return_value = ([], None)
        return db, a

    def test_unfenced_when_host_shows_no_lun(self):
        db, a = self._taken_over()
        # The host shows the LUN once the array presented it again.
        a.agent.lun_info.side_effect = [([], None), ([_lun_entry()], None)]
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [DEV_ID])
        self.assertEqual(FakeArrayPlugin.calls[-1], ("unfence", WWN, "host-node-a"))
        self.assertEqual(self.dev(db).fenced_node_ids, [])
        self.assertEqual(self.dev(db).serving_node(), "node-b")
        self.assertFalse(any("does not show the LUN" in e["message"] for e in self.events))

    def test_lun_not_visible_after_unfence_raises_an_event(self):
        # 2026-11-24 warm FTT2 bed: the kernel refused the reattached
        # namespace; the node counted as standby but could not take over.
        db, a = self._taken_over()
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [DEV_ID])
        self.assertTrue(any("does not show the LUN" in e["message"] for e in self.events))

    def test_stays_fenced_while_host_shows_lun(self):
        db, a = self._taken_over()
        a.agent.lun_info.return_value = ([_lun_entry()], None)
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])
        self.assertEqual(self.dev(db).fenced_node_ids, ["node-a"])

    def test_stays_fenced_when_inventory_fails(self):
        db, a = self._taken_over()
        a.agent.lun_info.return_value = (None, "boom")
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])
        self.assertEqual(self.dev(db).fenced_node_ids, ["node-a"])

    def test_stays_fenced_while_serving_node_is_down(self):
        db, a = self._taken_over()
        db.get_storage_node_by_id("node-b").status = StorageNode.STATUS_UNREACHABLE
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])
        self.assertEqual(self.dev(db).fenced_node_ids, ["node-a"])

    def test_not_while_node_is_not_online(self):
        db, a = self._taken_over()
        a.status = StorageNode.STATUS_UNREACHABLE
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])

    def test_stale_stack_is_retired_first(self):
        db, a = self._taken_over()
        dev = self.dev(db)
        a.spdk.bdevs[dev.alceml_bdev] = {"name": dev.alceml_bdev}
        a.spdk.bdevs[f"{dev.alceml_bdev}_PT"] = {"name": f"{dev.alceml_bdev}_PT"}
        with patch("simplyblock_core.db_controller.DBController", return_value=db):
            self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [DEV_ID])
        self.assertTrue(a.spdk.ops("bdev_alceml_delete"))
        self.assertEqual(self.dev(db).fenced_node_ids, [])


    def test_stays_fenced_while_its_array_fence_runs(self):
        # 2026-11-20 resize bed, R3: unfenced while the background detach
        # ran; the detach then left the node off the LUN for good.
        db, a = self._taken_over()
        key = (DEV_ID, "node-a")
        with lun_takeover._fences_guard:
            lun_takeover._fences_in_flight.add(key)
        try:
            self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])
        finally:
            with lun_takeover._fences_guard:
                lun_takeover._fences_in_flight.discard(key)
        self.assertEqual(self.dev(db).fenced_node_ids, ["node-a"])
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [DEV_ID])

    def test_stays_fenced_when_spdk_does_not_answer(self):
        # A node just lost can still read ONLINE; its dead SPDK proves
        # nothing about a stale stack.
        db, a = self._taken_over()
        a.spdk.bdev_get = MagicMock(side_effect=ConnectionError("refused"))
        self.assertEqual(lun_takeover.reintegrate_fenced_node(a), [])
        self.assertEqual(self.dev(db).fenced_node_ids, ["node-a"])
        a.agent.lun_info.assert_not_called()


class TestFenceAttachmentRecord(_TakeoverCase):
    """An array fence drops the node from array_attached_node_ids, so the
    attachment reconcile presents the LUN again once it is unfenced."""

    def test_array_fence_forgets_the_attachment(self):
        db, dev = _bed()
        dev.array_attached_node_ids = ["node-a", "node-b", "node-c"]
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).array_attached_node_ids, ["node-b", "node-c"])

    def test_background_fence_is_in_flight_until_done(self):
        db, dev = _bed()
        dev.lun_reservations = True
        dev.array_attached_node_ids = ["node-a", "node-b"]
        gate = threading.Event()
        fence = FakeArrayPlugin.fence

        def slow(plugin, *a):
            gate.wait(5)
            return fence(plugin, *a)
        b = db.nodes["node-b"]
        b.spdk.alceml_get_role = lambda name: {"role": "active", "state": "active"}
        with patch.object(FakeArrayPlugin, "fence", slow):
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
            self.assertTrue(lun_takeover.fence_in_flight(DEV_ID, "node-a"))
            gate.set()
            for _ in range(250):
                if not lun_takeover.fence_in_flight(DEV_ID, "node-a"):
                    break
                time.sleep(0.02)
        self.assertFalse(lun_takeover.fence_in_flight(DEV_ID, "node-a"))
        self.assertEqual(self.dev(db).array_attached_node_ids, ["node-b"])


class TestUnfenceOrder(_TakeoverCase):
    """The fence record is cleared before the array call: the attachment
    reconcile detaches any host the record still lists as fenced."""

    def test_record_cleared_before_array_attach(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        seen = []
        orig = FakeArrayPlugin.unfence

        def _unfence(plugin, lun, host):
            seen.append(list(self.dev(db).fenced_node_ids))
            return orig(plugin, lun, host)
        with patch.object(FakeArrayPlugin, "unfence", _unfence):
            self.assertTrue(lun_takeover.unfence_node(DEV_ID, "node-a"))
        self.assertEqual(seen, [[]])

    def test_array_failure_keeps_record_cleared(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        with patch.object(FakeArrayPlugin, "unfence", side_effect=RuntimeError("api")):
            self.assertTrue(lun_takeover.unfence_node(DEV_ID, "node-a"))
        self.assertEqual(self.dev(db).fenced_node_ids, [])


class TestTakeoverAfterGrowth(_TakeoverCase):
    """A node that kept the base bdev over a LUN from before a growth
    (device_controller.grow_cluster_devices) still has it at the old size;
    the takeover re-reads the block device instead of failing."""

    def _grown_bed(self):
        db, dev = _bed()
        dev.size = 2 * SIZE
        for nid in ("node-b", "node-c", "node-d"):
            db.nodes[nid].agent.lun_info.return_value = ([_lun_entry(size=2 * SIZE)], None)
        b = db.nodes["node-b"]
        base = utils.aio_bdev_name_for_serial(dev.serial_number)
        b.spdk.bdevs[base] = {"name": base, "block_size": 4096, "num_blocks": SIZE // 4096}
        return db, b, base

    def test_stale_base_bdev_is_rescanned(self):
        db, b, base = self._grown_bed()

        def _rescan(name):
            b.spdk.calls.append(("bdev_aio_rescan", (name,), {}))
            b.spdk.bdevs[name]["num_blocks"] = 2 * SIZE // 4096
            return True
        b.spdk.bdev_aio_rescan = _rescan
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(b.spdk.ops("bdev_aio_rescan"), [("bdev_aio_rescan", (base,), {})])
        b.agent.rescan_block_device.assert_called_once_with(BY_ID)
        self.assertEqual(b.spdk.ops("bdev_aio_create"), [])
        self.assertEqual(self.dev(db).serving_node(), "node-b")

    def test_base_bdev_still_small_after_rescan_fails_on_that_node(self):
        db, b, base = self._grown_bed()
        b.spdk.bdev_aio_rescan = lambda name: True
        self.assertNotEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(b.spdk.ops("bdev_alceml_create"), [])


# ------------------------------------- fence by persistent reservation

class TestFenceByReservation(_TakeoverCase):
    """ultra #236: the takeover's alceml opens with the standby's key and
    preempts the failed node's reservation (fence in ms); the array fence
    follows off the I/O path. Without the reservation: array fence first."""

    def _roles(self, node, *answers):
        seq = list(answers)
        node.spdk.alceml_get_role = lambda name: seq.pop(0) if len(seq) > 1 else seq[0]

    def _bed(self):
        db, dev = _bed()
        dev.lun_reservations = True
        return db, dev

    def _wait_background_fence(self):
        import time as _t
        for _ in range(100):
            if FakeArrayPlugin.calls:
                return
            _t.sleep(0.02)

    def test_reservation_fences_before_the_array(self):
        db, _ = self._bed()
        b = db.nodes["node-b"]
        self._roles(b, {"role": "activating", "state": "act_reservation"},
                    {"role": "active", "state": "active", "epoch": 2})
        order = []
        create = b.spdk.bdev_alceml_create
        b.spdk.bdev_alceml_create = lambda *a, **k: (order.append(("alceml", FakeArrayPlugin.calls[:])),
                                                     create(*a, **k))[1]
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        # Nothing at the array before the alceml (whose open preempts).
        self.assertEqual(order, [("alceml", [])])
        alceml = b.spdk.ops("bdev_alceml_create")[0]
        self.assertEqual(alceml[2]["reservation_key"], multi_attach.reservation_key(b))
        self.assertEqual(alceml[2]["reservation_key"], 2 << 32)
        self.assertNotIn("standby", alceml[2])
        self.assertEqual(self.dev(db).serving_node(), "node-b")
        self.assertIn("node-a", self.dev(db).fenced_node_ids)
        # The array fence follows in the background.
        self._wait_background_fence()
        self.assertEqual(FakeArrayPlugin.calls, [("fence", WWN, "host-node-a")])

    def test_failed_activation_falls_back_to_the_array_fence(self):
        db, _ = self._bed()
        b = db.nodes["node-b"]
        self._roles(b, {"role": "standby", "state": "act_failed"},
                    {"role": "active", "state": "active"})
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(FakeArrayPlugin.calls, [("fence", WWN, "host-node-a")])
        # The first instance was removed, the second one took the reservation.
        self.assertEqual(len(b.spdk.ops("bdev_alceml_create")), 2)
        self.assertEqual(len(b.spdk.ops("bdev_alceml_delete")), 1)
        self.assertEqual(self.dev(db).serving_node(), "node-b")

    def test_dead_standby_is_left_for_the_next_one(self):
        # 2026-11-20 FTT2 bed, dual reboot sn2 + hard reset sn3: the standby
        # chosen was rebooting; the LUN must still be taken over by another.
        db, _ = self._bed()
        b, c = db.nodes["node-b"], db.nodes["node-c"]

        def b_dies(name):
            b.spdk.down = True
            return {"error": "Could not reach remote"}
        b.spdk.alceml_get_role = b_dies
        self._roles(c, {"role": "active", "state": "active", "epoch": 3})
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 0.2):
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")
        self.assertIn("node-a", self.dev(db).fenced_node_ids)
        # The array fence ran (the reservation was not taken on node-b).
        self.assertIn(("fence", WWN, "host-node-a"), FakeArrayPlugin.calls)
        # No cleanup calls into the dead standby.
        self.assertEqual(b.spdk.ops("bdev_alceml_delete"), [])

    def test_dead_standby_costs_no_activation_wait(self):
        # 2026-11-20 FTT2 bed, case 7 again: the whole 5 s activation wait
        # and an array fence were spent on a rebooting standby (13 s stall).
        db, _ = self._bed()
        b, c = db.nodes["node-b"], db.nodes["node-c"]

        def b_gone(name):
            b.spdk.down = True
            raise ConnectionError("Could not reach remote")
        b.spdk.alceml_get_role = b_gone
        self._roles(c, {"role": "active", "state": "active", "epoch": 3})
        fenced_before_c = []
        create_c = c.spdk.bdev_alceml_create
        c.spdk.bdev_alceml_create = lambda *a, **k: (
            fenced_before_c.append(list(FakeArrayPlugin.calls)), create_c(*a, **k))[1]
        t0 = time.monotonic()
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 5):
            self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertLess(time.monotonic() - t0, 2.0)
        self.assertEqual(self.dev(db).serving_node(), "node-c")
        # node-c preempted by reservation: no array fence before it opened.
        self.assertEqual(fenced_before_c, [[]])
        self.assertEqual(len(b.spdk.ops("bdev_alceml_create")), 1)
        self._wait_background_fence()
        self.assertEqual(FakeArrayPlugin.calls, [("fence", WWN, "host-node-a")])

    def test_a_hanging_candidate_does_not_hold_the_choice(self):
        # 2026-11-20 FTT2 bed, network cuts of two nodes: the first candidate's
        # agent did not answer, the choice waited until the 90 s deadline.
        db, _ = self._bed()
        b, c = db.nodes["node-b"], db.nodes["node-c"]
        gate = threading.Event()
        b.agent.lun_info.side_effect = lambda *_a, **_k: (gate.wait(10), (None, "gone"))[1]
        self._roles(c, {"role": "active", "state": "active", "epoch": 3})
        t0 = time.monotonic()
        with patch.object(lun_takeover, "STANDBY_PROBE_TIMEOUT_SEC", 3.0):
            out = self.run_takeover(db, deadline_sec=4.0)
        gate.set()
        self.assertEqual(out, lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-c")
        self.assertLess(time.monotonic() - t0, 2.0)

    def test_candidate_order_wins_when_several_answer(self):
        db, _ = self._bed()
        db.nodes["node-b"].spdk.alceml_get_role = lambda name: {"role": "active", "state": "active"}
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        self.assertEqual(self.dev(db).serving_node(), "node-b")

    def test_no_standby_left_fails(self):
        db, _ = self._bed()
        def dies(spdk):
            def role(name):
                spdk.down = True
                return {"error": "gone"}
            return role
        for nid in ("node-b", "node-c", "node-d"):
            db.nodes[nid].spdk.alceml_get_role = dies(db.nodes[nid].spdk)
        with patch.object(constants, "MULTI_ATTACH_ACTIVATION_WAIT_SEC", 0.2):
            self.assertEqual(self.run_takeover(db), lun_takeover.FAILED)
        self.assertEqual(self.dev(db).serving_node(), "node-a")

    def test_without_reservations_the_array_fences_first(self):
        db, _ = _bed()
        self.assertEqual(self.run_takeover(db), lun_takeover.TAKEN_OVER)
        alceml = db.nodes["node-b"].spdk.ops("bdev_alceml_create")[0]
        self.assertNotIn("reservation_key", alceml[2])


class TestReservationKeys(unittest.TestCase):

    def setUp(self):
        array.forget_plugin()
        self.addCleanup(array.forget_plugin)

    def test_key_is_the_node_block_plus_one_in_the_upper_half(self):
        n = _node("n", "10.0.0.9", 0)
        self.assertEqual(multi_attach.reservation_key(n), 1 << 32)
        n.cntlid_block = 41
        self.assertEqual(multi_attach.reservation_key(n), 42 << 32)
        n.cntlid_block = -1
        self.assertEqual(multi_attach.reservation_key(n), 0)

    def test_kwargs_follow_the_device_record(self):
        db, dev = _bed()
        b = db.nodes["node-b"]
        self.assertNotIn("reservation_key", multi_attach.alceml_create_kwargs(dev, b, db.cluster))
        dev.lun_reservations = True
        kw = multi_attach.alceml_create_kwargs(dev, b, db.cluster)
        self.assertEqual(kw["reservation_key"], 2 << 32)
        self.assertEqual(kw["stuck_io_timeout_ms"], constants.MULTI_ATTACH_ALCEML_STUCK_IO_MS)
        dev.multi_attach = False
        self.assertEqual(multi_attach.alceml_create_kwargs(dev, b, db.cluster), {})

    def test_decided_with_the_array_capability_and_checksum(self):
        class PR(FakeArrayPlugin):
            def capabilities(self):
                return dict(super().capabilities(), supports_reservations=True)
        array.register_plugin("pr-test", PR)
        self.addCleanup(array.unregister_plugin, "pr-test")
        db, dev = _bed()
        self.assertFalse(multi_attach.reservations_supported(db.cluster, dev))   # fake: no PR
        db.cluster.array_plugin = "pr-test"
        array.forget_plugin()
        self.assertTrue(multi_attach.reservations_supported(db.cluster, dev))
        db.cluster.inline_checksum, dev.md_supported = True, False               # method 2
        self.assertFalse(multi_attach.reservations_supported(db.cluster, dev))
        with patch.object(constants, "MULTI_ATTACH_RESERVATIONS", False):
            db.cluster.inline_checksum = False
            self.assertFalse(multi_attach.reservations_supported(db.cluster, dev))
