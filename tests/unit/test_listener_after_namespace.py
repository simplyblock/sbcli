# coding=utf-8
"""A subsystem must not be reachable before its namespace is attached.

Regression: 2026-09-13-listener-published-before-namespace.

``add_lvol_on_node`` published the listener inside the ``resolve_subsys``
block and added the namespace afterwards, so between the two the subsystem
answered on the network while the lvol's namespace did not exist. A client
reading it got ``Invalid Namespace or Format (sct 0x0 / sc 0xb)`` with DNR
set, which the kernel does not retry on another path -- it fails the I/O
straight to the application.

k8s_native_rapid_failover_no_gap 2026-09-13, shared subsystem
``lvol:64b116a4`` rebuilt on worker-1 after its SPDK pod was force-deleted::

    15:50:04.722  nvmf_create_subsystem
    15:50:25.929  nvmf_subsystem_add_ns   nsid=1
    15:50:25.936  nvmf_subsystem_add_ns   nsid=2
    15:50:26.144  nvmf_subsystem_add_listener      <-- reachable here
    15:50:26.982  nvmf_subsystem_add_ns   nsid=3   <-- 838ms later

fio took ``err=121`` (EREMOTEIO) on the clone living at nsid 3, and the run
failed. Across that run 31.7% of listener publications were followed by a
namespace add within 10s (27.7% in the run before it, on the same build), so
the window is routine rather than exotic.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol


class ListenerOrderingTest(unittest.TestCase):

    def setUp(self):
        self.rpc = MagicMock(name="rpc")
        self.rpc.nvmf_subsystem_add_ns2.return_value = ("3", None)
        self.rpc.nvmf_subsystem_add_listener.return_value = (True, None)
        self.rpc.subsystem_create.return_value = True
        self.rpc.get_bdevs.return_value = [
            {"uuid": "lvol-bdev-uuid",
             "driver_specific": {"lvol": {"blobid": 33}}}]

        nic = MagicMock(name="nic")
        nic.ip4_address = "192.168.10.11"
        nic.trtype = "TCP"

        self.snode = MagicMock(name="snode")
        self.snode.rpc_client.return_value = self.rpc
        self.snode.data_nics = [nic]
        self.snode.active_tcp = True
        self.snode.get_id.return_value = "node-1"
        self.snode.get_lvol_subsys_port.return_value = 4436

        self.lvol = LVol()
        self.lvol.uuid = "lvol-1"
        self.lvol.nqn = "nqn.2023-02.io.simplyblock:cl:lvol:shared"
        self.lvol.top_bdev = "LVS_1/LVOL_9"
        self.lvol.lvs_name = "LVS_1"
        self.lvol.lvol_bdev = "LVOL_9"
        self.lvol.guid = "aabbccdd"
        self.lvol.lvol_uuid = "lvol-bdev-uuid"
        self.lvol.blobid = 33
        self.lvol.allowed_hosts = []
        self.lvol.fabric = "tcp"
        self.lvol.namespace = "shared-ns-group"
        self.lvol.node_id = "node-1"
        self.lvol.ns_id = 0

        self._patches = [
            patch.object(lvol_controller, "_create_bdev_stack",
                         return_value=(True, None)),
            patch.object(lvol_controller, "_resolve_namespaced_subsystem",
                         return_value=True),
            patch.object(lvol_controller, "DBController"),
            patch.object(lvol_controller, "_fail_after_bdev",
                         side_effect=lambda lvol, rpc, msg: (False, msg)),
            patch("simplyblock_core.controllers.migration_controller"
                  ".get_active_migration_for_nqn", return_value=None),
        ]
        for p in self._patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self._patches])

    def _call_order(self):
        return [c[0] for c in self.rpc.method_calls]

    def test_namespace_is_attached_before_the_listener_is_published(self):
        lvol_controller.add_lvol_on_node(self.lvol, self.snode)

        order = self._call_order()
        self.assertIn("nvmf_subsystem_add_ns2", order,
                      "the namespace was never added")
        self.assertIn("nvmf_subsystem_add_listener", order,
                      "the listener was never published")
        self.assertLess(
            order.index("nvmf_subsystem_add_ns2"),
            order.index("nvmf_subsystem_add_listener"),
            "the listener was published before the namespace existed: a client "
            "reaching the subsystem in that window gets Invalid Namespace or "
            "Format with DNR, which is not retried on another path")

    def test_a_listener_failure_detaches_the_namespace_it_attached(self):
        """Rollback must unwind the namespace, not just the bdev stack.

        Regression: 2026-09-13-listener-published-before-namespace (review).
        Moving the listener behind the namespace means a listener failure now
        happens with the namespace already attached. _fail_after_bdev only
        removes the bdev stack, so without this the namespace is left pointing
        at a bdev the rollback deletes -- reads on it answer INTERNAL DEVICE
        ERROR (the 2026-07-14 resurrected-namespace incident).
        """
        self.rpc.nvmf_subsystem_add_listener.return_value = (
            None, {"code": -1, "message": "listener add failed"})

        ok, _ = lvol_controller.add_lvol_on_node(self.lvol, self.snode)

        self.assertFalse(ok)
        self.rpc.nvmf_subsystem_remove_ns.assert_called_once_with(
            self.lvol.nqn, 3)

    def test_a_failed_namespace_add_publishes_no_listener(self):
        self.rpc.nvmf_subsystem_add_ns2.return_value = (
            None, {"code": -1, "message": "no slot"})
        self.rpc.subsystem_get.return_value = {"nqn": self.lvol.nqn}

        lvol_controller.add_lvol_on_node(self.lvol, self.snode)

        self.assertNotIn(
            "nvmf_subsystem_add_listener", self._call_order(),
            "a subsystem whose namespace add failed must not be made reachable")



class BatchListenerBarrierTest(unittest.TestCase):
    """A shared subsystem must not be reachable until every member is attached.

    Regression: 2026-09-13-listener-published-before-namespace.

    ``_register_lvols_on_node`` registers a node's lvols concurrently, and
    ``add_lvol_thread`` published each listener as soon as its own namespace
    landed. On a shared subsystem that makes the subsystem answer while the
    other members are still arriving -- the 838ms window measured on worker-1.
    The listeners belong after the future barrier, not inside the workers.
    """

    def test_no_listener_is_published_before_every_namespace_is_registered(self):
        from simplyblock_core import storage_node_ops as ops

        order = []

        def fake_add(lvol, snode, lvol_ana_state="optimized", defer_listener=False):
            order.append(("ns", lvol.get_id()))
            if not defer_listener:
                order.append(("listener", lvol.get_id()))
            return True, None

        def fake_publish(lvol, snode, rpc_client, lvol_ana_state):
            order.append(("listener", lvol.get_id()))
            return True, None

        lvols = []
        for i in range(3):
            lv = MagicMock(name="lvol%d" % i)
            lv.get_id.return_value = "lvol-%d" % i
            lv.nqn = "nqn.2023-02.io.simplyblock:cl:lvol:shared"
            lvols.append(lv)

        snode = MagicMock(name="snode")
        snode.get_id.return_value = "node-1"
        snode.rpc_client.return_value.subsystem_get.return_value = {"nqn": "x"}

        with patch.object(ops, "add_lvol_thread", side_effect=fake_add), \
             patch.object(ops, "_publish_lvol_listener", side_effect=fake_publish):
            ops._register_lvols_on_node(lvols, snode, "optimized")

        first_listener = next(i for i, (k, _) in enumerate(order) if k == "listener")
        namespaces_before = sum(1 for k, _ in order[:first_listener] if k == "ns")
        self.assertEqual(
            namespaces_before, len(lvols),
            "a listener was published after only %d of %d namespaces were "
            "registered: the subsystem is reachable while its remaining members "
            "are still arriving, and a read to one of those fails EREMOTEIO "
            "(order=%r)" % (namespaces_before, len(lvols), order))


class BatchSkipsFailedRegistrationsTest(unittest.TestCase):
    """A registration that failed must not get a listener.

    Regression: 2026-09-13-listener-published-before-namespace (review).
    ``failures`` includes lvols whose add_ns or namespace post-condition
    failed, so their namespace is exactly the one that may be absent.
    Publishing for them recreates the reachable-but-empty subsystem the
    barrier exists to prevent.
    """

    def test_a_failed_registration_gets_no_listener(self):
        from simplyblock_core import storage_node_ops as ops

        published = []

        def fake_add(lvol, snode, lvol_ana_state="optimized", defer_listener=False):
            if lvol.get_id() == "lvol-bad":
                return False, "add_ns failed"
            return True, None

        def fake_publish(lvol, snode, rpc_client, lvol_ana_state):
            published.append(lvol.get_id())
            return True, None

        lvols = []
        for name in ("lvol-good", "lvol-bad"):
            lv = MagicMock(name=name)
            lv.get_id.return_value = name
            lv.nqn = "nqn.2023-02.io.simplyblock:cl:lvol:shared"
            lvols.append(lv)

        snode = MagicMock(name="snode")
        snode.get_id.return_value = "node-1"
        snode.rpc_client.return_value.subsystem_get.return_value = {"nqn": "x"}

        with patch.object(ops, "add_lvol_thread", side_effect=fake_add), \
             patch.object(ops, "_publish_lvol_listener", side_effect=fake_publish):
            ops._register_lvols_on_node(lvols, snode, "optimized")

        self.assertNotIn(
            "lvol-bad", published,
            "a listener was published for an lvol whose registration failed: "
            "its namespace may never have attached, which is the empty-subsystem "
            "state this barrier exists to prevent")
        self.assertIn("lvol-good", published,
                      "the members that did register must still be published")


if __name__ == "__main__":
    unittest.main()
