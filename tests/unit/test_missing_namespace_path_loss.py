"""A listener must never be published in front of an empty subsystem.

K8sNativeResilientFailoverTest iteration 28 (2026-08-09): worker-3 carried 19
lvol subsystems that had a listener but no namespace (71 add_listener vs 52
add_ns on that node). That state is invisible to every layer above the
client kernel:

  * ``nvme connect`` succeeds and the target establishes qpairs;
  * the client prints ``new ctrl`` and never resets the controller, because
    nothing is actually wrong with it (no keep-alive timeout, no error
    recovery, no ``Removing ctrl``);
  * but the namespace never joins the multipath head, so the path does not
    exist for I/O routing.

Volume 638be965 therefore ran at 2 of 3 paths for 11 minutes, and when the
outage removed the two nodes holding those two paths the head had zero
members: ``block nvme0n1: no available path - failing I/O`` and ext4 went
read-only. The CSI side could not repair it either — its only remedy is
``nvme connect``, which the kernel answers with ``already connected``.

Contract pinned here: every registration path fails loudly instead of
publishing a reachable-but-empty subsystem, via BOTH routes that produced
one — an ``add_ns`` that failed, and an idempotency check that wrongly
reported the namespace already present.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol


def _make_lvol():
    lvol = LVol()
    lvol.uuid = "lvol-1"
    lvol.nqn = "nqn.2023-02.io.simplyblock:cl:lvol:638be965"
    lvol.top_bdev = "LVS_25/LVOL_9"
    lvol.lvs_name = "LVS_25"
    lvol.lvol_bdev = "LVOL_9"
    lvol.guid = "aabbccdd"
    lvol.lvol_uuid = "lvol-bdev-uuid"
    lvol.blobid = 33
    lvol.allowed_hosts = []
    lvol.node_id = "primary"
    lvol.nodes = ["primary", "secondary", "tertiary"]
    lvol.ns_id = 1
    lvol.status = LVol.STATUS_ONLINE
    return lvol


def _make_node(node_id="tertiary"):
    node = MagicMock(name="snode")
    node.get_id.return_value = node_id
    node.get_lvol_subsys_port.return_value = 4440
    node.data_nics = []
    node.cluster_id = "cluster-1"
    # Distinct from lvol.lvs_name ("LVS_25") so the node is a non-leader
    # host for it, with lvstore_ports already populated -- the normal,
    # settled state. Tests targeting the guard in
    # TestAddLvolThreadRefusesGuessedPort override this explicitly.
    node.lvstore = ""
    node.lvstore_ports = {"LVS_25": {"lvol_subsys_port": 4440, "hublvol_port": 4441}}
    return node


class TestRecreatePathRefusesEmptySubsystem(unittest.TestCase):
    """``recreate_lvol_on_node`` — the restart path."""

    def setUp(self):
        self.rpc = MagicMock(name="rpc")
        self.lvol = _make_lvol()
        self.snode = _make_node()
        self.snode.rpc_client.return_value = self.rpc

        nic = MagicMock()
        nic.ip4_address = "192.168.10.13"
        nic.trtype = "TCP"
        self.snode.data_nics = [nic]
        self.lvol.fabric = "tcp"

        p = patch.object(lvol_controller, "DBController")
        p.start()
        self.addCleanup(p.stop)

    def test_add_ns_failure_is_fatal_and_publishes_no_listener(self):
        self.rpc.nvmf_subsystem_add_ns.return_value = None

        ok, err = lvol_controller.recreate_lvol_on_node(
            self.lvol, self.snode, ha_inode_self=2)

        self.assertFalse(ok, "a failed namespace add must fail the recreate")
        self.assertTrue(err)
        self.rpc.listeners_create.assert_not_called()

    def test_successful_add_ns_still_publishes_listener(self):
        self.rpc.nvmf_subsystem_add_ns.return_value = 1

        ok, err = lvol_controller.recreate_lvol_on_node(
            self.lvol, self.snode, ha_inode_self=2)

        self.assertTrue(ok)
        self.assertIsNone(err)
        self.rpc.listeners_create.assert_called()


class TestAddLvolThreadRefusesEmptySubsystem(unittest.TestCase):
    """``add_lvol_thread`` — the create / restart-registration path."""

    def setUp(self):
        self.rpc = MagicMock(name="rpc")
        self.lvol = _make_lvol()
        self.snode = _make_node("secondary")
        self.snode.rpc_client.return_value = self.rpc

        nic = MagicMock()
        nic.ip4_address = "192.168.10.12"
        nic.trtype = "TCP"
        self.snode.data_nics = [nic]
        self.lvol.fabric = "tcp"
        self.lvol.lvol_type = "lvol"

        # Stop only OUR patcher. patch.stopall() would also tear down patches
        # started by other still-active fixtures (conftest included), which
        # silently breaks unrelated tests later in the run.
        p = patch.object(storage_node_ops, "DBController")
        db = p.start()
        self.addCleanup(p.stop)
        db.return_value.get_lvol_by_id.return_value = self.lvol
        db.return_value.get_pool_by_id.return_value.has_qos.return_value = False

    def test_refuses_listener_when_namespace_absent_after_add(self):
        # add_ns "succeeds" but the namespace is not actually present — this is
        # the shape produced by a false-positive idempotency check.
        self.rpc.nvmf_subsystem_add_ns.return_value = 1
        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          return_value=False):
            ok, err = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertFalse(ok, "no listener may be added to an empty subsystem")
        self.assertIn("no namespace", str(err))
        self.rpc.listeners_create.assert_not_called()

    def test_refuses_listener_when_add_ns_fails(self):
        self.rpc.nvmf_subsystem_add_ns.return_value = None
        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          return_value=False):
            ok, err = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertFalse(ok)
        self.rpc.listeners_create.assert_not_called()

    def test_publishes_listener_when_namespace_present(self):
        self.rpc.nvmf_subsystem_add_ns.return_value = 1
        # absent on the pre-check, present on the post-condition
        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          side_effect=[False, True]):
            ok, _ = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertTrue(ok)
        self.rpc.listeners_create.assert_called()


class TestAddLvolThreadRefusesGuessedPort(unittest.TestCase):
    """``add_lvol_thread`` must never fall back to snode's OWN leader port
    when registering a listener for a lvstore it hosts as a non-leader.

    Regression coverage for a real bug found live (2026-08-18): a
    node-removal relocation correctly repointed lvol.nodes to the new
    secondary, but lvol_monitor's repair loop raced the relocation's own
    lvstore_ports commit -- add_lvol_thread was handed a stale snode object
    (same "callers hold stale objects" hazard as the in_deletion check
    above) whose lvstore_ports had no entry yet for the lvol's lvstore.
    get_lvol_subsys_port()'s fallback-to-node-default (correct ONLY when
    lvs_name IS the node's own primary) silently returned the node's OWN
    leader port instead, and a listener was published on the wrong port
    with nothing ever revisiting or correcting it."""

    def setUp(self):
        self.rpc = MagicMock(name="rpc")
        self.lvol = _make_lvol()
        self.snode = _make_node("secondary")
        self.snode.rpc_client.return_value = self.rpc
        # This node leads its OWN lvstore (LVS_9) and is only a non-leader
        # host for the lvol's lvstore (LVS_25) -- the exact shape that hit
        # the bug live (zqmjp leading LVS_26 while hosting LVS_14 as a
        # freshly-relocated secondary).
        self.snode.lvstore = "LVS_9"

        nic = MagicMock()
        nic.ip4_address = "192.168.10.12"
        nic.trtype = "TCP"
        self.snode.data_nics = [nic]
        self.lvol.fabric = "tcp"
        self.lvol.lvol_type = "lvol"

        self.rpc.nvmf_subsystem_add_ns.return_value = 1

        p = patch.object(storage_node_ops, "DBController")
        self.db = p.start()
        self.addCleanup(p.stop)
        self.db.return_value.get_lvol_by_id.return_value = self.lvol
        self.db.return_value.get_pool_by_id.return_value.has_qos.return_value = False

    def test_refuses_when_lvstore_ports_missing_even_after_refetch(self):
        # Stale caller-held snode has no entry; a fresh re-fetch (simulated
        # here as the SAME missing state) confirms it's genuinely not ready.
        self.snode.lvstore_ports = {}
        fresh = _make_node("secondary")
        fresh.lvstore = "LVS_9"
        fresh.lvstore_ports = {}
        self.db.return_value.get_storage_node_by_id.return_value = fresh

        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          side_effect=[False, True]):
            ok, err = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertFalse(ok)
        self.assertIn("lvstore_ports", err)
        self.rpc.listeners_create.assert_not_called()
        # Must not have guessed using snode's OWN leader port either.
        self.snode.get_lvol_subsys_port.assert_not_called()

    def test_proceeds_with_fresh_port_once_refetch_finds_it(self):
        # Stale caller-held snode has no entry yet, but the relocation's
        # commit has already landed by the time we re-fetch -- must use
        # the FRESH object's real port, not snode's own leader port.
        self.snode.lvstore_ports = {}
        fresh = _make_node("secondary")
        fresh.lvstore = "LVS_9"
        fresh.lvstore_ports = {"LVS_25": {"lvol_subsys_port": 5150, "hublvol_port": 5151}}
        fresh.get_lvol_subsys_port = MagicMock(return_value=5150)
        fresh.rpc_client.return_value = self.rpc
        fresh.data_nics = self.snode.data_nics
        self.db.return_value.get_storage_node_by_id.return_value = fresh

        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          side_effect=[False, True]):
            ok, err = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertTrue(ok, err)
        self.rpc.listeners_create.assert_called_once()
        _, kwargs = self.rpc.listeners_create.call_args
        self.assertEqual(self.rpc.listeners_create.call_args.args[3], 5150)

    def test_own_leader_lvstore_never_triggers_the_guard(self):
        # The normal case: lvol.lvs_name IS this node's own primary
        # lvstore, which legitimately has no lvstore_ports entry -- must
        # use the plain node-level port without re-fetching or refusing.
        self.snode.lvstore = self.lvol.lvs_name
        self.snode.lvstore_ports = {}

        with patch.object(storage_node_ops, "_rpc_subsystem_has_ns",
                          side_effect=[False, True]):
            ok, err = storage_node_ops.add_lvol_thread(self.lvol, self.snode)

        self.assertTrue(ok, err)
        self.db.return_value.get_storage_node_by_id.assert_not_called()
        self.rpc.listeners_create.assert_called_once()
        self.assertEqual(self.rpc.listeners_create.call_args.args[3], 4440)


if __name__ == "__main__":
    unittest.main()
