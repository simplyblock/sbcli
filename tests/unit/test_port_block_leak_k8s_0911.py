# coding=utf-8
"""Port-block leak detection and the shutdown hublvol detach (k8s rapid
failover, 2026-09-11 13:46).

worker-3 (primary of LVS_16, port 4442) began a graceful shutdown at
13:46:02. Three things then went wrong, and all three are covered here:

1. The shutdown told the SECONDARY and the TERTIARY to
   ``bdev_nvme_detach_controller("LVS_16/hublvol")``. That controller is
   multipath — the tertiary's copy carries a path to the primary AND a path
   to the secondary — and a detach addressed by name alone drops every path,
   so the tertiary lost its hublvol outright. It promoted first, the
   secondary promoted after it, and the leader flap fenced the tertiary's
   4442 (SPDK ``lvol.c:block_port``) from 13:46:14 to 13:47:28.

2. The monitor dropped a peer-owned LVS port out of its check list whenever
   the owner was ONLINE or RESTARTING. worker-3 flipped to in_restart at
   13:47:05, 4442 left the list, and worker-5 was marked "online" again at
   13:47:08 while still fenced. The gate belongs on the health verdict, not
   on the observation: a fence nobody is looking at is a fence nobody can
   remediate.

3. ``port_block.is_port_blocked`` tested ``port_id in
   rpc.nvmf_get_blocked_ports()``, a ``{"blocked_ports": [...]}`` dict, so it
   compared against the dict's keys and returned False for every port.

The client budget this races is ctrl_loss_tmo = 30 x 2s = 60s, after which
the kernel deletes the controller and the namespace fails IO.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import health_controller
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor
from simplyblock_core.utils import port_block


# --------------------------------------------------------------------------
# 3. is_port_blocked parsed the wrong shape
# --------------------------------------------------------------------------

class TestIsPortBlockedParsesPayload(unittest.TestCase):
    """``nvmf_get_blocked_ports`` answers with a dict, not a list of ports."""

    @staticmethod
    def _node(payload):
        node = MagicMock()
        node.rpc_client.return_value.nvmf_get_blocked_ports.return_value = payload
        return node

    def test_blocked_port_reads_blocked(self):
        node = self._node({"blocked_ports": [{"port": 4442, "is_reject": False}]})
        self.assertTrue(port_block.is_port_blocked(node, 4442))

    def test_open_port_reads_open(self):
        node = self._node({"blocked_ports": [{"port": 4436}]})
        self.assertFalse(port_block.is_port_blocked(node, 4442))

    def test_no_blocked_ports_reads_open(self):
        self.assertFalse(port_block.is_port_blocked(self._node({}), 4442))
        self.assertFalse(
            port_block.is_port_blocked(self._node({"blocked_ports": []}), 4442))

    def test_string_port_is_accepted(self):
        node = self._node({"blocked_ports": [{"port": 4442}]})
        self.assertTrue(port_block.is_port_blocked(node, "4442"))

    def test_agrees_with_the_batched_reader(self):
        # The batched path always parsed this correctly, which is why the
        # monitor saw the fences and this function never did.
        payload = {"blocked_ports": [{"port": 4442}, {"port": 4436}]}
        node = self._node(payload)
        batched = port_block.get_blocked_ports_set(node)
        for port in (4434, 4436, 4442):
            self.assertEqual(port_block.is_port_blocked(node, port),
                             port in batched, port)

    def test_falls_back_to_iptables_when_rpc_absent(self):
        from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError

        node = MagicMock()
        node.rpc_client.return_value.nvmf_get_blocked_ports.side_effect = (
            RPCRemoteError("no such method", RPCErrorCode.method_not_found))
        with patch.object(port_block, "_is_port_blocked_iptables",
                          return_value=True) as legacy:
            self.assertTrue(port_block.is_port_blocked(node, 4442))
        legacy.assert_called_once()


# --------------------------------------------------------------------------
# 2. the skip gate suppressed the observation, not just the verdict
# --------------------------------------------------------------------------

def _node(node_id, *, status=StorageNode.STATUS_ONLINE, lvstore="LVS_1",
          port=4442, secondary=None, tertiary=None, is_secondary=False):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id.return_value = node_id
    n.status = status
    n.mgmt_ip = "10.0.0." + node_id[-1]
    n.lvstore = lvstore
    n.lvstore_status = "ready"
    n.nvmf_port = 4420
    n.is_secondary_node = is_secondary
    n.lvstore_stack_secondary = None
    n.lvstore_stack_tertiary = None
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    n.restart_phases = {}
    n.get_lvol_subsys_port.return_value = port
    return n


class TestPeerPortStaysObserved(unittest.TestCase):
    """A peer-owned LVS port is checked on every cycle. Whether it counts
    toward this node's health verdict is a separate question."""

    def setUp(self):
        storage_node_monitor._blocked_port_since.clear()
        self.addCleanup(storage_node_monitor._blocked_port_since.clear)

    def _run(self, owner_status):
        """worker-5 is the tertiary for worker-3's LVS_16 (port 4442), and
        4442 is fenced. Returns (verdict, port_results, owner_map)."""
        tertiary = _node("worker-5", lvstore="LVS_7", port=4436)
        tertiary.lvstore_stack_tertiary = "stack"
        owner = _node("worker-3", status=owner_status, lvstore="LVS_16",
                      port=4442, secondary="worker-4", tertiary="worker-5")

        db = MagicMock()
        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [owner]
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "worker-3": owner,
            "worker-4": _node("worker-4", status=StorageNode.STATUS_OFFLINE),
            "worker-5": tertiary,
        }[nid]

        seen = {}

        def _remediate(_db, _snode, port_results, port_lvs_owner):
            seen["results"] = dict(port_results)
            seen["owners"] = dict(port_lvs_owner)

        # 4442 fenced, this node's own ports open.
        results = {4420: True, 4436: True, 4442: False}
        with patch.object(storage_node_monitor, "db", db), \
             patch.object(storage_node_monitor.health_controller,
                          "check_ports_on_node",
                          side_effect=lambda _n, ports: {p: results[p] for p in ports}), \
             patch.object(storage_node_monitor, "_remediate_stale_port_blocks",
                          side_effect=_remediate), \
             patch.object(storage_node_monitor.health_controller,
                          "_check_ping_from_node", return_value=True):
            tertiary.data_nics = []
            verdict = storage_node_monitor.node_port_check_fun(tertiary)
        return verdict, seen.get("results", {}), seen.get("owners", {})

    def test_restarting_owner_port_is_still_checked_and_remediated(self):
        """13:47:05 — worker-3 goes in_restart. 4442 must NOT vanish."""
        verdict, results, owners = self._run(StorageNode.STATUS_RESTARTING)
        self.assertIn(4442, results)
        self.assertFalse(results[4442])
        self.assertEqual(owners.get(4442), "worker-3")

    def test_restarting_owner_port_does_not_flip_the_verdict(self):
        """The skip gate's original purpose survives: a fence authored by a
        restarting peer is not evidence that THIS node is unhealthy."""
        verdict, _results, _owners = self._run(StorageNode.STATUS_RESTARTING)
        self.assertTrue(verdict)

    def test_online_owner_port_is_advisory_too(self):
        verdict, results, _owners = self._run(StorageNode.STATUS_ONLINE)
        self.assertIn(4442, results)
        self.assertTrue(verdict)

    def test_offline_owner_port_still_counts_against_the_verdict(self):
        """Unchanged behaviour: with the owner offline and no other follower
        online, nothing legitimises the fence and the node is flagged."""
        verdict, results, _owners = self._run(StorageNode.STATUS_OFFLINE)
        self.assertIn(4442, results)
        self.assertFalse(verdict)


# --------------------------------------------------------------------------
# the remediation gates
# --------------------------------------------------------------------------

class TestStalePortBlockTimeout(unittest.TestCase):
    def test_timeout_leaves_room_for_a_missed_poll(self):
        # ~6s poll cadence against a 60s client ctrl_loss_tmo.
        self.assertLessEqual(storage_node_monitor.STALE_PORT_BLOCK_SEC, 12.0)
        self.assertGreaterEqual(storage_node_monitor.STALE_PORT_BLOCK_SEC, 10.0)


class TestRestartOwnsLvs(unittest.TestCase):
    """The restart phase is stamped on the node RUNNING the restart, keyed by
    the lvstore name — for a follower restart that is the FOLLOWER's record,
    not the primary's."""

    def _primary(self):
        return _node("worker-3", lvstore="LVS_16",
                     secondary="worker-4", tertiary="worker-5")

    def test_no_phase_anywhere_is_not_owned(self):
        primary = self._primary()
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda nid: _node(nid)
        self.assertFalse(health_controller._restart_owns_lvs(primary, db))

    def test_phase_on_the_primary_is_owned(self):
        primary = self._primary()
        primary.restart_phases = {"LVS_16": StorageNode.RESTART_PHASE_BLOCKED}
        self.assertTrue(health_controller._restart_owns_lvs(primary))

    def test_phase_on_a_restarting_follower_is_owned(self):
        primary = self._primary()
        tertiary = _node("worker-5")
        tertiary.restart_phases = {"LVS_16": StorageNode.RESTART_PHASE_BLOCKED}
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "worker-4": _node("worker-4"), "worker-5": tertiary}[nid]
        self.assertFalse(health_controller._restart_owns_lvs(primary))
        self.assertTrue(health_controller._restart_owns_lvs(primary, db))

    def test_unreadable_follower_counts_as_owned(self):
        primary = self._primary()
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = KeyError("gone")
        self.assertTrue(health_controller._restart_owns_lvs(primary, db))

    def test_phase_for_another_lvstore_is_not_owned(self):
        primary = self._primary()
        primary.restart_phases = {"LVS_7": StorageNode.RESTART_PHASE_BLOCKED}
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda nid: _node(nid)
        self.assertFalse(health_controller._restart_owns_lvs(primary, db))


# --------------------------------------------------------------------------
# 1. the shutdown hublvol detach
# --------------------------------------------------------------------------

class TestShutdownDoesNotDetachPeerHublvol(unittest.TestCase):
    """Source-level invariant. ``shutdown_storage_node`` must not detach the
    hublvol controller on the secondary or the tertiary: addressed by name
    alone the detach drops every path of a multipath controller, including
    the follower's path to the peer that is about to lead."""

    @staticmethod
    def _function_source(name):
        import os
        root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        with open(os.path.join(root, "simplyblock_core",
                               "storage_node_ops.py"), encoding="utf-8") as f:
            src = f.read()
        start = src.index(f"\ndef {name}(")
        end = src.find("\ndef ", start + 1)
        return src[start:end if end != -1 else len(src)]

    def test_shutdown_issues_no_hublvol_detach(self):
        body = self._function_source("shutdown_storage_node")
        code = "\n".join(line for line in body.splitlines()
                         if not line.lstrip().startswith("#"))
        self.assertNotIn("bdev_nvme_detach_controller", code)
        self.assertNotIn("hublvol.bdev_name", code)

    def test_deliberate_replica_teardown_still_detaches(self):
        """The detach is correct where the replica really is going away."""
        for name in ("teardown_non_leader_lvstore", "_delete_replica_on_peer"):
            self.assertIn("bdev_nvme_detach_controller",
                          self._function_source(name), name)


if __name__ == "__main__":
    unittest.main()
