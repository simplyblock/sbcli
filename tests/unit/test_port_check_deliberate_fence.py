"""A deliberate, sub-second fence on a node's own lvstore port is not a
port-down.

Run 50, 2026-10-01 15:16:46. Removing 8hhg5 relocated the secondary of htthx's
LVS_10 onto jj7dr. Building it there is the follower-restart sequence, which
fences the leader's client port while the new secondary attaches: port 4445
on htthx was blocked from 15:16:46.358 to 15:16:46.754 (0.394s). The monitor's
routine port check sampled htthx at 15:16:46.665, read 4445 blocked, and
set_node_down marked a healthy node DOWN for 7s -- a DOWN that is broadcast to
every distrib.

Two things now stand between one blocked sample and set_node_down:

* a restart phase for the lvstore on ANY node explains the block. jj7dr
  carried ``LVS_10: blocked`` from 15:16:46.038, but htthx's record still
  named the removed 8hhg5 as its secondary, so looking only at the named
  followers would have missed it;
* failing that, a second read a moment later must agree. A fence that has
  already lifted is not evidence against the node.

A real fence -- still blocked on the re-read, no phase owning it -- still
flips the verdict, and a leaked phase (one get_restart_phase clears) does not
exempt the port.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import health_controller
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as snm

OWN_PORT = 4445


def _node(node_id, *, lvstore="LVS_10", port=OWN_PORT, secondary=None,
          restart_phases=None, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id.return_value = node_id
    n.cluster_id = "c1"
    n.status = status
    n.mgmt_ip = "10.0.128.25"
    n.lvstore = lvstore
    n.lvstore_status = "ready"
    n.nvmf_port = 4420
    n.is_secondary_node = False
    n.lvstore_stack_secondary = None
    n.lvstore_stack_tertiary = None
    n.secondary_node_id = secondary
    n.tertiary_node_id = None
    n.restart_phases = restart_phases or {}
    n.data_nics = []
    n.get_lvol_subsys_port.return_value = port
    return n


class TestOwnPortFence(unittest.TestCase):

    def setUp(self):
        snm._blocked_port_since.clear()
        self.addCleanup(snm._blocked_port_since.clear)

    def _check(self, reads, *, rewiring_node_phase=None, phase_lookup=""):
        """htthx (own LVS_10 on 4445) reads its own port as ``reads`` on
        successive checks. jj7dr, not named as htthx's follower, optionally
        carries a restart phase for LVS_10."""
        htthx = _node("htthx", secondary="8hhg5-removed")
        phases = {"LVS_10": rewiring_node_phase} if rewiring_node_phase else {}
        jj7dr = _node("jj7dr", lvstore="LVS_3", port=4423, restart_phases=phases)

        db = MagicMock()
        db.get_primary_storage_nodes_by_secondary_node_id.return_value = []
        db.get_storage_nodes_by_cluster_id.return_value = [htthx, jj7dr]

        reads = list(reads)
        calls = []

        def _ports(_n, ports):
            calls.append(list(ports))
            own = reads.pop(0) if reads else True
            return {p: (own if p == OWN_PORT else True) for p in ports}

        seen = {}

        def _remediate(_db, _snode, port_results, _owners):
            seen["results"] = dict(port_results)

        sleep = MagicMock()
        with patch.object(snm, "db", db), \
             patch.object(snm.health_controller, "check_ports_on_node", side_effect=_ports), \
             patch.object(snm, "_remediate_stale_port_blocks", side_effect=_remediate), \
             patch.object(snm.storage_node_ops, "get_restart_phase", return_value=phase_lookup), \
             patch.object(snm.time, "sleep", sleep):
            verdict = snm.node_port_check_fun(htthx)
        return verdict, calls, sleep, seen.get("results", {})

    def test_the_run_50_relocation_fence_is_not_a_port_down(self):
        """jj7dr is building htthx's LVS_10 (phase ``blocked``) while htthx
        still names the removed node as its secondary."""
        verdict, calls, sleep, _ = self._check(
            [False], rewiring_node_phase=StorageNode.RESTART_PHASE_BLOCKED,
            phase_lookup=StorageNode.RESTART_PHASE_BLOCKED)
        self.assertTrue(verdict)
        sleep.assert_not_called()
        self.assertEqual(len(calls), 1, "a phase-explained block needs no re-read")

    def test_a_block_gone_on_the_re_read_is_not_a_port_down(self):
        """The sample landed in the fence after its phase had already cleared."""
        verdict, calls, sleep, results = self._check([False, True])
        self.assertTrue(verdict)
        sleep.assert_called_once_with(snm.PORT_BLOCK_CONFIRM_SEC)
        self.assertEqual(calls[-1], [OWN_PORT])
        self.assertIs(results[OWN_PORT], True,
                      "a lifted block must not start the stale-fence clock")

    def test_a_fence_still_there_on_the_re_read_is_a_port_down(self):
        verdict, _calls, _sleep, results = self._check([False, False])
        self.assertFalse(verdict)
        self.assertIs(results[OWN_PORT], False)

    def test_a_leaked_phase_does_not_exempt_the_port(self):
        """jj7dr's record carries a phase no flow owns any more:
        get_restart_phase clears it and answers "not in restart"."""
        verdict, _calls, sleep, _ = self._check(
            [False, False], rewiring_node_phase=StorageNode.RESTART_PHASE_BLOCKED,
            phase_lookup="")
        self.assertFalse(verdict)
        sleep.assert_called_once()

    def test_an_open_port_costs_nothing_extra(self):
        verdict, calls, sleep, _ = self._check([True])
        self.assertTrue(verdict)
        sleep.assert_not_called()
        self.assertEqual(len(calls), 1)

    def test_the_re_read_fits_inside_the_port_check_budget(self):
        self.assertLess(snm.PORT_BLOCK_CONFIRM_SEC, snm.PORT_CHECK_JOIN_TIMEOUT_SEC / 2)


class TestRestartOwnsLvsLooksBeyondTheNamedFollowers(unittest.TestCase):
    """_restart_owns_lvs gates the stale-fence remediation. It looked only at
    the primary and its named followers, so it missed the same relocation."""

    def test_a_relocation_target_not_yet_named_owns_the_lvs(self):
        htthx = _node("htthx", secondary="8hhg5-removed")
        removed = _node("8hhg5-removed", lvstore="LVS_2")
        jj7dr = _node("jj7dr", lvstore="LVS_3",
                      restart_phases={"LVS_10": StorageNode.RESTART_PHASE_BLOCKED})
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = {"8hhg5-removed": removed}.get
        db.get_storage_nodes_by_cluster_id.return_value = [htthx, removed, jj7dr]
        self.assertTrue(health_controller._restart_owns_lvs(htthx, db))

    def test_no_phase_anywhere_owns_nothing(self):
        htthx = _node("htthx", secondary="jj7dr")
        jj7dr = _node("jj7dr", lvstore="LVS_3")
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = {"jj7dr": jj7dr}.get
        db.get_storage_nodes_by_cluster_id.return_value = [htthx, jj7dr]
        self.assertFalse(health_controller._restart_owns_lvs(htthx, db))

    def test_without_a_db_only_the_primary_is_asked(self):
        htthx = _node("htthx")
        self.assertFalse(health_controller._restart_owns_lvs(htthx))
