"""A corrupt JM no longer leaves its node stuck in restart.

On restart the JM stack is re-opened in load mode (alceml pba_init_mode=1,
then bdev_jm_create). A corrupt alceml page map or journal fails one of the two
with EFAULT ("Bad address") on every attempt, so the restart failed forever.
The restart path now wipes the JM once -- alceml re-created formatted
(pba_init_mode=3), JM fresh -- when every journal on it has an online copy on
another JM; JC then resyncs it. Never on other errors, never without HA copies,
never more than once.
"""
import errno
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import jm_raid, storage_node_ops
from simplyblock_core.models.nvme_device import JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError


def _efault():
    return RPCRemoteError("Bad address", code=-errno.EFAULT)


class _Rpc:
    """Records calls; keeps the set of bdevs that exist."""

    def __init__(self, bdevs, jm_errors=()):
        self.bdevs = set(bdevs)
        self.jm_errors = list(jm_errors)
        self.calls = []

    def bdev_get(self, name):
        if name in self.bdevs:
            return {"name": name, "block_size": 512, "num_blocks": 1000, "aliases": ["u"]}
        return None

    def bdev_jm_create(self, name, alceml, **kw):
        self.calls.append(("bdev_jm_create", name, alceml))
        if self.jm_errors:
            err = self.jm_errors.pop(0)
            if err is not None:
                # A failed JM create frees the JM; the alceml under it stays.
                raise err
        self.bdevs.add(name)
        return name

    def bdev_jm_delete(self, name, safe_removal=False):
        self.calls.append(("bdev_jm_delete", name))
        self.bdevs.discard(name)
        return True

    def bdev_alceml_delete(self, name):
        self.calls.append(("bdev_alceml_delete", name))
        self.bdevs.discard(name)
        return True

    def bdev_raid_create(self, name, members, level):
        self.bdevs.add(name)
        return True

    def bdev_PT_NoExcl_create(self, name, base):
        self.bdevs.add(name)
        return True

    def subsystem_create(self, *a, **kw):
        return True

    def nvmf_subsystem_add_ns(self, *a, **kw):
        return True

    def listeners_create(self, *a, **kw):
        return True


def _jm(uuid="jm-a", status=JMDevice.STATUS_ONLINE, **kw):
    jm = JMDevice()
    jm.uuid = uuid
    jm.status = status
    for k, v in kw.items():
        setattr(jm, k, v)
    return jm


def _node(uuid, jm_uuid, jm_ids=(), status=StorageNode.STATUS_ONLINE, ha=True, ha_count=3):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "c1"
    n.status = status
    n.enable_ha_jm = ha
    n.ha_jm_count = ha_count
    n.jm_ids = list(jm_ids)
    n.jm_device = _jm(jm_uuid) if jm_uuid else None
    return n


def _snode(alceml_results, rpc):
    """The restarting node A; its own journal is also on JMs of B and C."""
    snode = MagicMock()
    snode.get_id.return_value = "A"
    snode.cluster_id = "c1"
    snode.enable_ha_jm = True
    snode.ha_jm_count = 3
    snode.jm_ids = ["jm-b", "jm-c"]
    snode.site = ""
    snode.data_nics = []
    snode.subsystem = "nqn:A"
    snode.nvmf_port = 4420
    snode.jm_device = _jm("jm-a", raid_bdev="raid_jm_A", jm_leg_bdevs=["p1", "p2"],
                          jm_leg_members=[["p1"], ["p2"]])
    results = list(alceml_results)
    modes = []

    def create_alceml(name, base, uuid, pba_init_mode, **kw):
        modes.append(pba_init_mode)
        res = results.pop(0) if results else True
        if isinstance(res, BaseException):
            raise res
        rpc.bdevs.add(name)
        return True

    snode.create_alceml.side_effect = create_alceml
    return snode, modes


class _Base(unittest.TestCase):
    def setUp(self):
        self.peers = [
            _node("B", "jm-b", jm_ids=["jm-c", "jm-a"]),
            _node("C", "jm-c", jm_ids=["jm-a", "jm-b"]),
        ]
        self.cluster = MagicMock()
        self.cluster.jm_raid_layout = jm_raid.LAYOUT_RAID01
        self.cluster.page_size_in_blocks = 2097152
        self.cluster.full_page_unmap = False
        self.cluster.shared_placement = False
        db = patch.object(storage_node_ops, "DBController")
        self.db = db.start().return_value
        self.addCleanup(db.stop)
        self.db.get_cluster_by_id.return_value = self.cluster
        self.db.get_storage_nodes_by_cluster_id.side_effect = lambda *_a, **_k: self.peers
        ev = patch.object(storage_node_ops, "storage_events")
        self.events = ev.start()
        self.addCleanup(ev.stop)
        sl = patch.object(storage_node_ops.time, "sleep")
        sl.start()
        self.addCleanup(sl.stop)

    def _raid(self, alceml_results=(), jm_errors=(), wipe=True, after_restart=True):
        rpc = _Rpc({"p1", "p2"}, jm_errors=jm_errors)
        snode, modes = _snode(alceml_results, rpc)
        ret = storage_node_ops._create_jm_stack_on_raid(
            rpc, ["p1", "p2"], snode, after_restart=after_restart, wipe_on_corruption=wipe)
        return ret, rpc, modes


class TestCorruptionSignal(unittest.TestCase):
    def test_efault_code_is_corruption(self):
        self.assertTrue(storage_node_ops.is_jm_corruption_error(_efault()))

    def test_efault_as_strerror_under_generic_code_is_corruption(self):
        self.assertTrue(storage_node_ops.is_jm_corruption_error(
            RPCRemoteError("Bad address", code=-32603)))

    def test_other_errors_are_not(self):
        for err in (RPCRemoteError("No such device", code=-errno.ENODEV),
                    RPCRemoteError("File exists", code=-errno.EEXIST),
                    RPCRemoteError("Device or resource busy", code=-errno.EBUSY),
                    RPCRemoteError("Cannot allocate memory", code=-errno.ENOMEM),
                    RPCRemoteError("Invalid argument", code=-errno.EINVAL),
                    RPCConnectionError("Did not get a response from remote within timeout"),
                    RuntimeError("Bad address")):
            self.assertFalse(storage_node_ops.is_jm_corruption_error(err), err)


class TestWipeOnRestart(_Base):
    def test_corrupt_alceml_is_wiped_and_jm_created_fresh(self):
        ret, rpc, modes = self._raid(alceml_results=[_efault()])
        self.assertIsInstance(ret, JMDevice)
        self.assertEqual(modes, [1, 3])
        self.assertEqual([c for c in rpc.calls if c[0] == "bdev_jm_create"],
                         [("bdev_jm_create", "jm_A", "alceml_jm_A")])
        self.events.jm_wiped_after_corruption.assert_called_once()
        self.assertEqual(self.events.jm_wiped_after_corruption.call_args[0][1], "alceml load")
        self.events.jm_corrupt_not_wiped.assert_not_called()

    def test_corrupt_journal_deletes_alceml_then_recreates_both(self):
        ret, rpc, modes = self._raid(jm_errors=[_efault()])
        self.assertIsInstance(ret, JMDevice)
        self.assertEqual(modes, [1, 3])
        names = [c[0] for c in rpc.calls]
        self.assertEqual(names, ["bdev_jm_create", "bdev_alceml_delete", "bdev_jm_create"])
        self.events.jm_wiped_after_corruption.assert_called_once()
        self.assertEqual(self.events.jm_wiped_after_corruption.call_args[0][1], "JM journal load")

    def test_leftover_jm_bdev_is_deleted_before_the_wipe(self):
        rpc = _Rpc({"p1", "p2"}, jm_errors=[_efault()])
        snode, modes = _snode([], rpc)
        orig = rpc.bdev_jm_create

        def jm_create_leaves_bdev(name, alceml, **kw):
            try:
                return orig(name, alceml, **kw)
            except RPCRemoteError:
                rpc.bdevs.add(name)
                raise

        rpc.bdev_jm_create = jm_create_leaves_bdev
        ret = storage_node_ops._create_jm_stack_on_raid(
            rpc, ["p1", "p2"], snode, after_restart=True, wipe_on_corruption=True)
        self.assertIsInstance(ret, JMDevice)
        names = [c[0] for c in rpc.calls]
        self.assertEqual(names, ["bdev_jm_create", "bdev_jm_delete", "bdev_alceml_delete",
                                 "bdev_jm_create"])

    def test_transient_error_is_not_wiped(self):
        for err in (RPCConnectionError("Could not reach remote"),
                    RPCRemoteError("No such device", code=-errno.ENODEV),
                    RPCRemoteError("File exists", code=-errno.EEXIST)):
            with self.assertRaises(type(err)):
                self._raid(alceml_results=[err])
            with self.assertRaises(type(err)):
                self._raid(jm_errors=[err])
        self.events.jm_wiped_after_corruption.assert_not_called()
        self.events.jm_corrupt_not_wiped.assert_not_called()

    def test_failed_fresh_create_does_not_loop(self):
        with self.assertRaises(RPCRemoteError):
            _, _, _ = self._raid(alceml_results=[_efault(), _efault()])
        rpc = _Rpc({"p1", "p2"}, jm_errors=[_efault(), _efault(), None])
        snode, modes = _snode([], rpc)
        with self.assertRaises(RPCRemoteError):
            storage_node_ops._create_jm_stack_on_raid(
                rpc, ["p1", "p2"], snode, after_restart=True, wipe_on_corruption=True)
        self.assertEqual(modes, [1, 3])
        self.assertEqual(len([c for c in rpc.calls if c[0] == "bdev_jm_create"]), 2)
        self.events.jm_wiped_after_corruption.assert_not_called()

    def test_failed_fresh_alceml_create_attempts_once(self):
        rpc = _Rpc({"p1", "p2"})
        snode, modes = _snode([_efault(), _efault(), True], rpc)
        with self.assertRaises(RPCRemoteError):
            storage_node_ops._create_jm_stack_on_raid(
                rpc, ["p1", "p2"], snode, after_restart=True, wipe_on_corruption=True)
        self.assertEqual(modes, [1, 3])

    def test_not_wiped_without_the_restart_opt_in(self):
        with self.assertRaises(RPCRemoteError):
            self._raid(alceml_results=[_efault()], wipe=False)
        self.events.jm_corrupt_not_wiped.assert_not_called()

    def test_missing_backing_bdev_is_not_wiped(self):
        rpc = _Rpc({"p1", "p2"})
        snode, modes = _snode([_efault()], rpc)
        real_raid_create = rpc.bdev_raid_create

        def raid_create(name, members, level):
            real_raid_create(name, members, level)
            if name == "raid_jm_A":
                rpc.bdevs.discard(name)   # vanished: the alceml could not open it
            return True

        rpc.bdev_raid_create = raid_create
        with self.assertRaises(RPCRemoteError):
            storage_node_ops._create_jm_stack_on_raid(
                rpc, ["p1", "p2"], snode, after_restart=True, wipe_on_corruption=True)
        self.assertEqual(modes, [1])
        self.events.jm_wiped_after_corruption.assert_not_called()


class TestWipeSafety(_Base):
    def test_no_wipe_without_ha_jm_and_operator_is_told(self):
        rpc = _Rpc({"p1", "p2"})
        snode, modes = _snode([_efault()], rpc)
        snode.enable_ha_jm = False
        with self.assertRaises(RPCRemoteError):
            storage_node_ops._create_jm_stack_on_raid(
                rpc, ["p1", "p2"], snode, after_restart=True, wipe_on_corruption=True)
        self.assertEqual(modes, [1])
        self.events.jm_corrupt_not_wiped.assert_called_once()
        self.assertIn("HA JM", self.events.jm_corrupt_not_wiped.call_args[0][3])
        self.events.jm_wiped_after_corruption.assert_not_called()

    def test_no_wipe_when_peer_jms_are_offline(self):
        for p in self.peers:
            p.status = StorageNode.STATUS_OFFLINE
        with self.assertRaises(RPCRemoteError):
            self._raid(alceml_results=[_efault()])
        self.events.jm_corrupt_not_wiped.assert_called_once()
        self.events.jm_wiped_after_corruption.assert_not_called()

    def test_safe_with_one_online_remote_copy_of_every_journal(self):
        snode = MagicMock(enable_ha_jm=True, ha_jm_count=3, jm_ids=["jm-b", "jm-c"], site="",
                          cluster_id="c1", jm_device=_jm("jm-a"))
        snode.get_id.return_value = "A"
        self.peers[1].status = StorageNode.STATUS_OFFLINE     # C down, B still has copies
        self.assertEqual(storage_node_ops.jm_wipe_safety(snode), (True, ""))

    def test_unsafe_when_own_remote_jm_is_offline(self):
        snode = MagicMock(enable_ha_jm=True, ha_jm_count=2, jm_ids=["jm-b"], site="",
                          cluster_id="c1", jm_device=_jm("jm-a"))
        snode.get_id.return_value = "A"
        self.peers[0].jm_device.status = JMDevice.STATUS_UNAVAILABLE
        ok, reason = storage_node_ops.jm_wipe_safety(snode)
        self.assertFalse(ok)
        self.assertIn("this node's journal", reason)

    def test_unsafe_when_this_jm_is_the_last_reachable_copy_of_a_peer_journal(self):
        # D's journal: D's own JM + jm-a. D is offline, so jm-a is its only reachable copy.
        self.peers.append(_node("D", "jm-d", jm_ids=["jm-a"], status=StorageNode.STATUS_OFFLINE,
                                ha_count=2))
        snode = MagicMock(enable_ha_jm=True, ha_jm_count=3, jm_ids=["jm-b", "jm-c"], site="",
                          cluster_id="c1", jm_device=_jm("jm-a"))
        snode.get_id.return_value = "A"
        ok, reason = storage_node_ops.jm_wipe_safety(snode)
        self.assertFalse(ok)
        self.assertIn("D", reason)

    def test_unused_jm_id_beyond_ha_count_is_not_a_copy(self):
        snode = MagicMock(enable_ha_jm=True, ha_jm_count=2, jm_ids=["jm-c", "jm-b"], site="",
                          cluster_id="c1", jm_device=_jm("jm-a"))
        snode.get_id.return_value = "A"
        self.peers[1].status = StorageNode.STATUS_OFFLINE     # jm-c: the only used remote
        ok, _ = storage_node_ops.jm_wipe_safety(snode)
        self.assertFalse(ok)


class TestRestartSingleDeviceJm(_Base):
    """The non-raid JM path of _prepare_cluster_devices_on_restart."""

    def _prepare(self, alceml_results=(), jm_errors=(), clear_data=False):
        rpc = _Rpc({"nvme_jm"}, jm_errors=jm_errors)
        snode, modes = _snode(alceml_results, rpc)
        snode.nvme_devices = []
        snode.enable_test_device = False
        snode.enable_ha_jm = True
        snode.jm_device = _jm("jm-a", nvme_bdev="nvme_jm", alceml_bdev="alceml_jm-a",
                              jm_nvme_bdev_list=[])
        snode.rpc_client.return_value = rpc
        with patch.object(storage_node_ops, "apply_jc_dual_node"):
            ret = storage_node_ops._prepare_cluster_devices_on_restart(snode, clear_data=clear_data)
        return ret, rpc, modes, snode

    def test_corrupt_alceml_is_wiped(self):
        ret, rpc, modes, snode = self._prepare(alceml_results=[_efault()])
        self.assertTrue(ret)
        self.assertEqual(modes, [1, 3])
        self.assertEqual(snode.jm_device.status, JMDevice.STATUS_ONLINE)
        self.events.jm_wiped_after_corruption.assert_called_once()

    def test_corrupt_journal_is_wiped(self):
        ret, rpc, modes, _ = self._prepare(jm_errors=[_efault()])
        self.assertTrue(ret)
        self.assertEqual(modes, [1, 3])
        self.assertIn(("bdev_alceml_delete", "alceml_jm-a"), rpc.calls)

    def test_clean_restart_loads_once(self):
        ret, rpc, modes, _ = self._prepare()
        self.assertTrue(ret)
        self.assertEqual(modes, [1])
        self.events.jm_wiped_after_corruption.assert_not_called()

    def test_clear_data_formats_and_never_wipes_again(self):
        with self.assertRaises(RPCRemoteError):
            self._prepare(alceml_results=[_efault()], clear_data=True)


if __name__ == "__main__":
    unittest.main()
