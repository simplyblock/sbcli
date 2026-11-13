"""Multi-attach array LUN mode, phase 2 (simplyblock_core.multi_attach).

  - cluster create/add options: --multi-attach needs --raid-devices, --ftt,
    ha_type 'ha'; forces 1+0 and max_fault_tolerance = ftt; --ftt alone is
    refused; the regular npcs>=2 rule for FTT2 does not block multi-attach.
  - floors: nodes >= FTT+1 (>= 3 for FTT2), >= 1 data device per node.
  - device model defaults and the lblk LUN marking.
  - host-independent NQN, derived namespace UUID, per-node cntlid ranges.
  - RPC wrappers: max_cntlid and stuck_io_timeout_ms sent only when given.
  - device stacks (storage_node_ops and device_controller): NQN, cntlid
    range, namespace UUID, alceml stuck-I/O timeout; unchanged otherwise.
  - peer attach: hold timeouts and multipath mode only for multi-attach.
  - owner loopback: cluster map and remote-device connect.
"""

import json
import unittest
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

from simplyblock_core import (
    cluster_ops,
    constants,
    distr_controller,
    floors,
    multi_attach,
    storage_node_ops,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

DEV_ID = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"


def _device(dev_id=DEV_ID, multi=False, node_id="node-a", order=0):
    d = NVMeDevice()
    d.uuid = dev_id
    d.node_id = node_id
    d.status = NVMeDevice.STATUS_ONLINE
    d.alceml_bdev = f"alceml_{dev_id}"
    d.alceml_name = f"alceml_{dev_id}"
    d.cluster_device_order = order
    d.size = 8 * 1024 ** 3
    d.multi_attach = multi
    return d


def _ma_cluster(ftt=1):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.nqn = f"{constants.CLUSTER_NQN}:cluster-1"
    cl.distr_ndcs, cl.distr_npcs = 1, 0
    cl.raid_devices = True
    cl.multi_attach = True
    cl.ha_type = "ha"
    cl.max_fault_tolerance = ftt
    cl.status = Cluster.STATUS_ACTIVE
    return cl


# ---------------------------------------------------------------- options

class TestResolveOptions(unittest.TestCase):

    def _resolve(self, **kw):
        args = dict(multi_attach=True, ftt=1, raid_devices=True, ha_type="ha",
                    is_single_node=False, distr_ndcs=2, distr_npcs=1,
                    max_fault_tolerance=1)
        args.update(kw)
        return cluster_ops._resolve_multi_attach(**args)

    def test_regular_cluster_is_unchanged(self):
        self.assertEqual(self._resolve(multi_attach=False, ftt=None), (2, 1, 1))

    def test_ftt_without_multi_attach_is_refused(self):
        with self.assertRaisesRegex(ValueError, "only valid together with multi_attach"):
            self._resolve(multi_attach=False, ftt=2)

    def test_forces_one_plus_zero_and_ftt(self):
        self.assertEqual(self._resolve(ftt=1), (1, 0, 1))
        self.assertEqual(self._resolve(ftt=2, distr_ndcs=4, distr_npcs=2), (1, 0, 2))

    def test_requires_raid_devices(self):
        with self.assertRaisesRegex(ValueError, "raid_devices"):
            self._resolve(raid_devices=False)

    def test_requires_ftt(self):
        for ftt in (None, 0, 3):
            with self.subTest(ftt=ftt), self.assertRaisesRegex(ValueError, "ftt 1 or 2"):
                self._resolve(ftt=ftt)

    def test_requires_ha(self):
        with self.assertRaisesRegex(ValueError, "ha_type='ha'"):
            self._resolve(ha_type="single")

    def test_refuses_single_node(self):
        with self.assertRaisesRegex(ValueError, "single-node"):
            self._resolve(is_single_node=True)


def _add_cluster(**kw):
    """Run _add_cluster_impl with the persistence and side effects stubbed;
    returns the Cluster it wrote."""
    written = []
    db = MagicMock()
    db.get_clusters.return_value = [MagicMock(cluster_name="other")]
    cfg = MagicMock()
    cfg.mode = "docker"
    cfg.db_connection = ""
    cfg.grafana_secret = cluster_ops.SecretStr("")
    cfg.grafana_endpoint = ""
    cfg.disable_monitoring = True
    db.get_deploy_config.return_value = cfg
    args = dict(blk_size=4096, page_size_in_blocks=2097152, cap_warn=0, cap_crit=0,
                prov_cap_warn=0, prov_cap_crit=0, distr_ndcs=1, distr_npcs=1,
                distr_bs=4096, distr_chunk_bs=4096, ha_type="ha", enable_node_affinity=False,
                qpair_count=256, max_queue_size=128, inflight_io_threshold=4,
                strict_node_anti_affinity=False, is_single_node=False, name="",
                max_subsys=10, spdk_vcpu_count=4)
    args.update(kw)
    with patch.object(cluster_ops, "db_controller", db), \
            patch.object(cluster_ops, "cluster_events"), \
            patch.object(cluster_ops, "validate_spdk_sizing"), \
            patch.object(Cluster, "write_to_db", autospec=True,
                         side_effect=lambda self, *a, **k: written.append(self)):
        cluster_ops._add_cluster_impl(**args)
    return written[-1]


class TestAddCluster(unittest.TestCase):

    def test_multi_attach_ftt2_is_stored_as_one_plus_zero(self):
        cl = _add_cluster(multi_attach=True, ftt=2, raid_devices=True,
                          distr_ndcs=1, distr_npcs=1, max_fault_tolerance=1)
        self.assertTrue(cl.multi_attach)
        self.assertTrue(cl.raid_devices)
        self.assertEqual((cl.distr_ndcs, cl.distr_npcs), (1, 0))
        self.assertEqual(cl.max_fault_tolerance, 2)

    def test_regular_cluster_keeps_its_scheme(self):
        cl = _add_cluster(distr_ndcs=2, distr_npcs=1, max_fault_tolerance=1)
        self.assertFalse(cl.multi_attach)
        self.assertEqual((cl.distr_ndcs, cl.distr_npcs, cl.max_fault_tolerance), (2, 1, 1))

    def test_regular_ftt2_still_needs_two_parity_chunks(self):
        with self.assertRaisesRegex(ValueError, "distr_npcs >= 2"):
            _add_cluster(distr_ndcs=2, distr_npcs=1, max_fault_tolerance=2)

    def test_multi_attach_without_raid_devices_is_refused(self):
        with self.assertRaisesRegex(ValueError, "raid_devices"):
            _add_cluster(multi_attach=True, ftt=1)

    def test_add_cluster_passes_the_options_through(self):
        with patch.object(cluster_ops, "_add_cluster_impl", return_value="c1") as impl:
            cluster_ops.add_cluster(4096, 2097152, 0, 0, 0, 0, 1, 1, 4096, 4096, "ha", False,
                                    256, 128, 4, False, False, "", multi_attach=True, ftt=2,
                                    raid_devices=True)
        kw = impl.call_args.kwargs
        self.assertEqual((kw["multi_attach"], kw["ftt"]), (True, 2))

    def test_create_cluster_refuses_ftt_alone(self):
        with self.assertRaisesRegex(ValueError, "only valid together"):
            cluster_ops.create_cluster(
                4096, 2097152, "", 0, 0, 0, 0, "eth0", "10.0.0.1", 0, 0, "", "", 1, 1,
                4096, 4096, "ha", "docker", False, 256, 256, 128, 4, True, False, "",
                "", "", "", "tcp", False, "", ftt=1)


class TestCli(unittest.TestCase):

    def _parse(self, *argv):
        from simplyblock_cli import cli as cli_module
        wrapper = cli_module.CLIWrapper.__new__(cli_module.CLIWrapper)
        cli_module.CLIWrapper.__init__(wrapper)
        return wrapper.parser.parse_args(list(argv))

    def test_options_on_create_and_add(self):
        for command in ("create", "add"):
            with self.subTest(command=command):
                args = self._parse("cluster", command, "--raid-devices", "--multi-attach", "--ftt", "2")
                self.assertTrue(args.multi_attach)
                self.assertEqual(args.ftt, 2)
                plain = self._parse("cluster", command)
                self.assertFalse(plain.multi_attach)
                self.assertIsNone(plain.ftt)

    def test_ftt_choices(self):
        with self.assertRaises(SystemExit):
            self._parse("cluster", "add", "--ftt", "3")


class TestApiV2(unittest.TestCase):

    def test_params_carry_the_options(self):
        from simplyblock_web.api.v2.cluster import ClusterParams
        p = ClusterParams(max_subsys=10, spdk_vcpu_count=4, raid_devices=True,
                          multi_attach=True, ftt=2)
        dumped = p.model_dump(exclude_none=True)
        self.assertEqual((dumped["multi_attach"], dumped["ftt"]), (True, 2))
        self.assertNotIn("ftt", ClusterParams(max_subsys=10, spdk_vcpu_count=4).model_dump(exclude_none=True))


# ---------------------------------------------------------------- floors

def _node(uid, devices=1, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = uid
    n.cluster_id = "cluster-1"
    n.status = status
    n.nvme_devices = [_device(f"{uid}-d{i}", multi=True, node_id=uid) for i in range(devices)]
    return n


class TestFloors(unittest.TestCase):

    def test_node_floor_is_ftt_plus_one(self):
        f1 = floors.cluster_floors(_ma_cluster(1), False, member_nodes=2)
        self.assertEqual((f1.nodes, f1.devices_per_node, f1.data_devices), (2, 1, 1))
        f2 = floors.cluster_floors(_ma_cluster(2), False, member_nodes=2)
        self.assertEqual(f2.nodes, 3)
        self.assertIn("multi-attach", f2.nodes_rule)

    def test_no_single_or_two_node_override(self):
        self.assertEqual(floors.cluster_floors(_ma_cluster(2), False, member_nodes=1).nodes, 3)

    def test_violations_name_the_rule(self):
        cl = _ma_cluster(2)
        v = floors.evaluate(cl, [_node("a"), _node("b")])
        self.assertTrue(any("FTT+1" in x for x in v), v)
        v = floors.evaluate(cl, [_node("a"), _node("b"), _node("c", devices=0)])
        self.assertEqual(len(v), 1)
        self.assertIn("one data LUN per node is recommended", v[0])
        self.assertEqual(floors.evaluate(cl, [_node("a"), _node("b", devices=2), _node("c")]), [])

    def test_regular_violation_text_unchanged(self):
        cl = _ma_cluster(1)
        cl.multi_attach = False
        cl.distr_ndcs, cl.distr_npcs = 1, 1
        v = floors.evaluate(cl, [_node("a"), _node("b"), _node("c"), _node("d", devices=0)])
        self.assertIn("node d has 0 online data devices < floor 1 (data devices per node)", v)


# ------------------------------------------------------- device model/NQN

class TestDeviceModel(unittest.TestCase):

    def test_defaults_are_off(self):
        d = NVMeDevice()
        self.assertEqual((d.multi_attach, d.lun_wwn, d.serving_node_id, d.node_paths),
                         (False, "", "", {}))
        d.node_id = "n1"
        self.assertEqual(d.serving_node(), "n1")
        d.serving_node_id = "n2"
        self.assertEqual(d.serving_node(), "n2")

    def test_record_round_trips(self):
        d = _device(multi=True)
        d.lun_wwn = "0x600a"
        d.node_paths = {"node-a": {"path": "/dev/disk/by-id/wwn-0x600a", "bdev": "aio_x"}}
        back = NVMeDevice(json.loads(json.dumps(d.to_dict())))
        self.assertEqual((back.multi_attach, back.lun_wwn, back.node_paths),
                         (True, "0x600a", d.node_paths))

    def _devs(self):
        a = NVMeDevice({"uuid": "a", "serial_number": "SER-A", "nvme_bdev": "aio_a",
                        "by_id_path": "/dev/disk/by-id/wwn-0x1", "device_path": "/dev/sdb"})
        b = NVMeDevice({"uuid": "b", "serial_number": "SER-B", "nvme_bdev": "aio_b",
                        "device_path": "/dev/sdc"})
        j = NVMeDevice({"uuid": "j", "serial_number": "SER-J", "nvme_bdev": "aio_j"})
        entries = [{"serial": "SER-A", "wwn": "0x1"}, {"serial": "SER-B"},
                   {"serial": "SER-J", "journal": True}]
        return a, b, j, entries

    def test_mark_lun_devices(self):
        a, b, j, entries = self._devs()
        snode = MagicMock()
        snode.get_id.return_value = "node-a"
        multi_attach.mark_lun_devices(_ma_cluster(), snode, [a, b, j], entries)
        self.assertTrue(a.multi_attach and b.multi_attach)
        self.assertEqual((a.lun_wwn, b.lun_wwn), ("0x1", "SER-B"))
        self.assertEqual(a.node_paths, {"node-a": {"path": "/dev/disk/by-id/wwn-0x1", "bdev": "aio_a"}})
        self.assertEqual(b.node_paths["node-a"]["path"], "/dev/sdc")
        self.assertFalse(j.multi_attach)

    def test_mark_is_a_no_op_on_a_regular_cluster(self):
        a, b, j, entries = self._devs()
        cl = _ma_cluster()
        cl.multi_attach = False
        multi_attach.mark_lun_devices(cl, MagicMock(), [a, b], entries)
        self.assertFalse(a.multi_attach or b.multi_attach)
        self.assertEqual((a.lun_wwn, a.node_paths), ("", {}))

    def test_resolve_lblk_entries_carries_the_wwn(self):
        from simplyblock_core import utils
        live = {"name": "sdb", "device_path": "/dev/sdb", "serial": "SER-A", "wwn": "0x1",
                "by_id_path": "/dev/disk/by-id/wwn-0x1", "size": 1, "type": "disk"}
        resolved, missing = utils.resolve_lblk_entries([{"serial": "SER-A", "name": "sdb"}], [live])
        self.assertEqual((resolved[0]["wwn"], missing), ("0x1", []))


class TestNames(unittest.TestCase):

    def test_device_nqn_is_host_independent(self):
        cl = _ma_cluster()
        snode = MagicMock(subsystem=f"{constants.CLUSTER_NQN}:host1_8080")
        nqn = multi_attach.device_subsystem_nqn(snode, _device(multi=True), cl)
        self.assertEqual(nqn, f"{constants.CLUSTER_NQN}:lun:{DEV_ID}")
        self.assertEqual(multi_attach.device_subsystem_nqn(snode, _device(), cl),
                         f"{constants.CLUSTER_NQN}:host1_8080:dev:{DEV_ID}")

    def test_namespace_uuid(self):
        self.assertEqual(multi_attach.namespace_uuid(_device()), DEV_ID)
        ns = multi_attach.namespace_uuid(_device(multi=True))
        self.assertNotEqual(ns, DEV_ID)
        self.assertEqual(ns, multi_attach.namespace_uuid(_device(multi=True)))
        uuid_mod.UUID(ns)

    def test_cntlid_ranges(self):
        self.assertEqual(multi_attach.cntlid_range(0), (1, 64))
        self.assertEqual(multi_attach.cntlid_range(1), (65, 128))
        lo, hi = multi_attach.cntlid_range(constants.MULTI_ATTACH_MAX_NODES - 1)
        self.assertLessEqual(hi, 0xFFEF)
        for bad in (-1, constants.MULTI_ATTACH_MAX_NODES):
            with self.subTest(block=bad), self.assertRaises(ValueError):
                multi_attach.cntlid_range(bad)

    def test_kwargs_are_empty_for_regular_devices(self):
        d = _device()
        self.assertEqual(multi_attach.attach_controller_kwargs(d), {})
        self.assertEqual(multi_attach.alceml_create_kwargs(d), {})
        self.assertEqual(multi_attach.subsystem_create_kwargs(MagicMock(), d), {})
        self.assertEqual(multi_attach.attach_controller_kwargs(MagicMock()), {})

    def test_hold_and_stuck_io_constants(self):
        self.assertLess(constants.MULTI_ATTACH_ALCEML_STUCK_IO_MS, constants.MULTI_ATTACH_HOLD_SEC * 1000)
        self.assertEqual(multi_attach.attach_controller_kwargs(_device(multi=True)), {
            "ctrlr_loss_timeout_sec": constants.MULTI_ATTACH_HOLD_SEC, "reconnect_delay_sec": 2,
            "fast_io_fail_timeout_sec": constants.MULTI_ATTACH_HOLD_SEC})
        self.assertEqual(multi_attach.alceml_create_kwargs(_device(multi=True)),
                         {"stuck_io_timeout_ms": 60000})


class TestCntlidBlockClaim(unittest.TestCase):

    def _claim(self, alloc, node_id="n3", live=("n1", "n2", "n3")):
        from simplyblock_core.db_controller import DBController, _NoTxnStore
        kv = {}
        if alloc is not None:
            kv[b"alloc"] = json.dumps(alloc).encode()
        store = MagicMock()
        store.get.side_effect = kv.get
        store.set.side_effect = kv.__setitem__
        db = DBController.__new__(DBController)
        node = StorageNode()
        node.uuid = node_id
        with patch.object(DBController, "_atomic_update_tx",
                          side_effect=lambda tr, key, cls, fn, idx: fn(node)):
            block = db._claim_cntlid_block_tx(_NoTxnStore(store), b"node", b"alloc", node_id,
                                              set(live), 4, ())
        return block, node, json.loads(kv[b"alloc"]) if b"alloc" in kv else None

    def test_smallest_free_block(self):
        block, node, alloc = self._claim({"n1": 0, "n2": 2})
        self.assertEqual((block, node.cntlid_block), (1, 1))
        self.assertEqual(alloc, {"n1": 0, "n2": 2, "n3": 1})

    def test_first_claim_and_idempotence(self):
        self.assertEqual(self._claim(None)[0], 0)
        block, _, alloc = self._claim({"n3": 2, "n1": 0})
        self.assertEqual((block, alloc), (2, {"n3": 2, "n1": 0}))

    def test_gone_nodes_release_their_block(self):
        block, _, alloc = self._claim({"gone": 0, "n1": 1}, live=("n1", "n3"))
        self.assertEqual(block, 0)
        self.assertNotIn("gone", alloc)

    def test_full(self):
        with self.assertRaisesRegex(ValueError, "at most 4"):
            self._claim({"n1": 0, "n2": 1, "x": 2, "y": 3}, live=("n1", "n2", "n3", "x", "y"))

    def test_ensure_uses_an_existing_block(self):
        node = StorageNode()
        node.cntlid_block = 5
        db = MagicMock()
        self.assertEqual(multi_attach.ensure_cntlid_block(node, db), 5)
        db.claim_cntlid_block.assert_not_called()
        node.cntlid_block = -1
        db.claim_cntlid_block.return_value = 7
        self.assertEqual(multi_attach.ensure_cntlid_block(node, db), 7)
        self.assertEqual(node.cntlid_block, 7)


# ---------------------------------------------------------------- RPC

class TestRpcWrappers(unittest.TestCase):

    def _params(self, call):
        c = RPCClient("127.0.0.1", 8080, "u", cluster_ops.SecretStr("p"))
        with patch.object(c, "_request", return_value=True) as req:
            call(c)
        return req.call_args.kwargs

    def test_subsystem_create_max_cntlid_only_when_given(self):
        p = self._params(lambda c: c.subsystem_create("nqn", "sn", "mn"))
        self.assertNotIn("max_cntlid", p)
        self.assertEqual(p["min_cntlid"], 1)
        p = self._params(lambda c: c.subsystem_create("nqn", "sn", "mn", min_cntlid=65, max_cntlid=128))
        self.assertEqual((p["min_cntlid"], p["max_cntlid"]), (65, 128))

    def test_alceml_stuck_io_only_when_non_zero(self):
        p = self._params(lambda c: c.bdev_alceml_create("a", "b", DEV_ID))
        self.assertNotIn("stuck_io_timeout_ms", p)
        p = self._params(lambda c: c.bdev_alceml_create("a", "b", DEV_ID, stuck_io_timeout_ms=60000))
        self.assertEqual(p["stuck_io_timeout_ms"], 60000)


# ---------------------------------------------------------- device stacks

def _stack_snode():
    snode = MagicMock()
    snode.subsystem = f"{constants.CLUSTER_NQN}:host1_8080"
    snode.enable_test_device = False
    snode.cntlid_block = 2
    snode.nvmf_port = 4420
    iface = MagicMock(ip4_address="10.0.0.1", trtype="TCP", listen_address="10.0.0.1")
    snode.data_nics = [iface]
    return snode


class TestStorageDeviceStack(unittest.TestCase):

    def _run(self, dev, cluster):
        snode = _stack_snode()
        rpc = MagicMock()
        rpc.bdev_get.return_value = {"aliases": ["pt-uuid"]}
        with patch.object(storage_node_ops, "DBController") as db:
            db.return_value.get_cluster_by_id.return_value = cluster
            out = storage_node_ops._create_storage_device_stack(rpc, dev, snode, False)
        self.assertIs(out, dev)
        return snode, rpc

    def test_multi_attach_device(self):
        dev = _device(multi=True)
        snode, rpc = self._run(dev, _ma_cluster())
        nqn = f"{constants.CLUSTER_NQN}:lun:{DEV_ID}"
        rpc.subsystem_create.assert_called_once_with(nqn, "sbcli-cn", DEV_ID, min_cntlid=129, max_cntlid=192)
        rpc.nvmf_subsystem_add_ns.assert_called_once_with(nqn, f"alceml_{DEV_ID}_PT",
                                                          multi_attach.namespace_uuid(dev),
                                                          nguid=multi_attach.namespace_nguid(dev))
        self.assertEqual(snode.create_alceml.call_args.kwargs["stuck_io_timeout_ms"], 60000)
        self.assertEqual(dev.nvmf_nqn, nqn)

    def test_regular_device_is_unchanged(self):
        dev = _device()
        cl = _ma_cluster()
        cl.multi_attach = False
        snode, rpc = self._run(dev, cl)
        nqn = f"{constants.CLUSTER_NQN}:host1_8080:dev:{DEV_ID}"
        rpc.subsystem_create.assert_called_once_with(nqn, "sbcli-cn", DEV_ID)
        rpc.nvmf_subsystem_add_ns.assert_called_once_with(nqn, f"alceml_{DEV_ID}_PT", DEV_ID)
        self.assertNotIn("stuck_io_timeout_ms", snode.create_alceml.call_args.kwargs)


class TestDeviceControllerStack(unittest.TestCase):

    def _run(self, dev, cluster):
        from simplyblock_core.controllers import device_controller
        snode = _stack_snode()
        rpc = MagicMock()
        rpc.bdev_get.return_value = None
        rpc.subsystem_get.return_value = None
        snode.rpc_client.return_value = rpc
        with patch.object(device_controller, "DBController") as db:
            db.return_value.get_cluster_by_id.return_value = cluster
            self.assertTrue(device_controller._def_create_device_stack(dev, snode))
        return snode, rpc

    def test_multi_attach_device(self):
        dev = _device(multi=True)
        snode, rpc = self._run(dev, _ma_cluster())
        nqn = f"{constants.CLUSTER_NQN}:lun:{DEV_ID}"
        rpc.subsystem_create.assert_called_once_with(nqn, "sbcli-cn", DEV_ID, min_cntlid=129, max_cntlid=192)
        rpc.nvmf_subsystem_add_ns.assert_called_once_with(nqn, f"alceml_{DEV_ID}_PT",
                                                          multi_attach.namespace_uuid(dev),
                                                          nguid=multi_attach.namespace_nguid(dev))
        self.assertEqual(snode.create_alceml.call_args.kwargs["stuck_io_timeout_ms"], 60000)
        self.assertEqual(dev.nvmf_nqn, nqn)

    def test_regular_device_is_unchanged(self):
        dev = _device()
        cl = _ma_cluster()
        cl.multi_attach = False
        snode, rpc = self._run(dev, cl)
        nqn = f"{constants.CLUSTER_NQN}:host1_8080:dev:{DEV_ID}"
        rpc.subsystem_create.assert_called_once_with(nqn, "sbcli-cn", DEV_ID)
        rpc.nvmf_subsystem_add_ns.assert_called_once_with(nqn, f"alceml_{DEV_ID}_PT")
        self.assertNotIn("stuck_io_timeout_ms", snode.create_alceml.call_args.kwargs)


# ---------------------------------------------------------------- attach

class TestPeerAttach(unittest.TestCase):

    def _attach(self, dev):
        dev.nvmf_ip = "10.0.0.5"
        dev.nvmf_nqn = "nqn.x"
        dev.nvmf_port = 4420
        target = MagicMock(active_rdma=False, active_tcp=True)
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = []
        rpc.bdev_get.return_value = {"name": "b"}
        attach = MagicMock()
        attach.bdev_nvme_attach_controller.return_value = [f"remote_{dev.alceml_bdev}n1"]
        with patch.object(storage_node_ops, "DBController") as db:
            db.return_value.get_storage_node_by_id.return_value = target
            storage_node_ops._connect_device_attach(
                f"remote_{dev.alceml_bdev}", dev, MagicMock(), rpc, attach, ["10.0.0.5"], False)
        return attach.bdev_nvme_attach_controller.call_args

    def test_multi_attach_holds_io(self):
        call = self._attach(_device(multi=True))
        self.assertEqual(call.kwargs["multipath"], "multipath")
        self.assertEqual(call.kwargs["ctrlr_loss_timeout_sec"], constants.MULTI_ATTACH_HOLD_SEC)
        self.assertEqual(call.kwargs["reconnect_delay_sec"], constants.MULTI_ATTACH_RECONNECT_DELAY_SEC)
        self.assertEqual(call.kwargs["fast_io_fail_timeout_sec"], constants.MULTI_ATTACH_FAST_IO_FAIL_SEC)

    def test_regular_device_is_unchanged(self):
        call = self._attach(_device())
        self.assertEqual(call.kwargs, {"multipath": False})


# -------------------------------------------------------------- loopback

def _map_node(uid, devices):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = uid
    n.cluster_id = "cluster-1"
    n.status = StorageNode.STATUS_ONLINE
    n.is_secondary_node = False
    n.failure_domain = -1
    n.nvme_devices = devices
    n.remote_devices = []
    return n


class TestOwnerLoopback(unittest.TestCase):

    def _map(self, node):
        with patch.object(distr_controller, "DBController") as db:
            cl = MagicMock(enable_node_affinity=False, enable_failure_domain=False,
                           sync_replication=False)
            db.return_value.get_cluster_by_id.return_value = cl
            return distr_controller.get_distr_cluster_map([node], node, "distr-1")

    def _remote(self, dev):
        from simplyblock_core.models.nvme_device import RemoteDevice
        rd = RemoteDevice()
        rd.uuid = dev.uuid
        rd.remote_bdev = f"remote_{dev.alceml_bdev}n1"
        return rd

    def test_own_multi_attach_device_uses_the_loopback_bdev(self):
        dev = _device(multi=True)
        node = _map_node("node-a", [dev])
        node.remote_devices = [self._remote(dev)]
        entry = self._map(node)["map_cluster"]["node-a"]["devices"][0]
        self.assertEqual(entry["bdev_name"], f"remote_alceml_{DEV_ID}n1")
        self.assertEqual(entry["status"], NVMeDevice.STATUS_ONLINE)

    def test_own_multi_attach_device_without_loopback_is_unavailable(self):
        node = _map_node("node-a", [_device(multi=True)])
        entry = self._map(node)["map_cluster"]["node-a"]["devices"][0]
        self.assertEqual(entry["status"], NVMeDevice.STATUS_UNAVAILABLE)

    def test_own_regular_device_uses_the_local_alceml(self):
        node = _map_node("node-a", [_device()])
        entry = self._map(node)["map_cluster"]["node-a"]["devices"][0]
        self.assertEqual(entry["bdev_name"], f"alceml_{DEV_ID}")

    def test_add_device_map_uses_the_loopback_bdev(self):
        dev = _device(multi=True)
        target = _map_node("node-a", [dev])
        target.rpc_client.return_value.distr_add_devices.return_value = True
        with patch.object(distr_controller, "DBController") as db:
            db.return_value.get_storage_node_by_id.return_value = target
            with patch.object(distr_controller, "_sync_replication", return_value=False):
                distr_controller.send_cluster_map_add_device(dev, target)
        cl_map = target.rpc_client.return_value.distr_add_devices.call_args.args[0]
        self.assertEqual(cl_map["devices"][0]["bdev_name"], f"remote_alceml_{DEV_ID}n1")

    def _connect(self, own_devices, peer_devices):
        this = _map_node("node-a", own_devices)
        peer = _map_node("node-b", peer_devices)
        rpc = MagicMock()
        rpc.bdev_get.side_effect = lambda name: {"name": name} if name.startswith("remote_") else None
        this.rpc_client.return_value = rpc
        connected = []
        with patch.object(storage_node_ops, "DBController") as db, \
                patch.object(storage_node_ops, "_connect_device_thread",
                             side_effect=lambda name, dev, node: connected.append(name)), \
                patch.object(storage_node_ops, "_fetch_bdev_name_set", return_value=set()):
            db.return_value.get_storage_nodes_by_cluster_id.return_value = [this, peer]
            out = storage_node_ops._connect_to_remote_devs(this)
        return connected, {rd.get_id() for rd in out}

    def test_connect_includes_own_multi_attach_devices(self):
        own = _device("own-1", multi=True, node_id="node-a")
        peer = _device("peer-1", multi=True, node_id="node-b", order=1)
        connected, ids = self._connect([own], [peer])
        self.assertEqual(sorted(connected), ["remote_alceml_own-1", "remote_alceml_peer-1"])
        self.assertEqual(ids, {"own-1", "peer-1"})

    def test_connect_skips_own_regular_devices(self):
        own = _device("own-1", node_id="node-a")
        peer = _device("peer-1", node_id="node-b", order=1)
        connected, ids = self._connect([own], [peer])
        self.assertEqual(connected, ["remote_alceml_peer-1"])
        self.assertEqual(ids, {"peer-1"})


if __name__ == "__main__":
    unittest.main()


class TestNamespaceNguid(unittest.TestCase):
    """The NGUID of a multi-attach namespace is set explicitly: SPDK would
    otherwise fill it from the PT bdev's name-derived UUID, which collides on
    any node that also consumes the LUN."""

    def test_regular_device_has_none(self):
        self.assertIsNone(multi_attach.namespace_nguid(_device()))

    def test_multi_attach_nguid_is_the_namespace_uuid(self):
        import uuid as uuid_mod
        dev = _device(multi=True)
        nguid = multi_attach.namespace_nguid(dev)
        self.assertEqual(nguid, uuid_mod.UUID(multi_attach.namespace_uuid(dev)).hex.upper())
        self.assertEqual(len(nguid), 32)

    def test_differs_from_the_pt_bdev_uuid(self):
        # bdev_ptnonexcl.c: uuid5(7e25812e-...-000000000001, "<alceml>_PT").
        import uuid as uuid_mod
        dev = _device(multi=True)
        pt = uuid_mod.uuid5(uuid_mod.UUID("7e25812e-c8c0-4d3f-8599-000000000001"), f"alceml_{DEV_ID}_PT")
        self.assertNotEqual(multi_attach.namespace_nguid(dev), pt.hex.upper())
